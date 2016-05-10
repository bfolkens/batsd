module Batsd
  # 
  # Abstract interface for handling different types of data
  # (e.g., counters, timers, etc.). 
  #
  # Generally, this should be subclassed to provide the specific 
  # functionality desired. If left unmodified, it provides an echo
  # handler when run with <code>ENV["VVERBOSE"]</code>, and is silent otherwise.
  #
  class Handler
   
    # Creates a new handler object and spawns a threadpool. If
    # <code>options[:threadpool_size]</code> is specified, that will be used
    # (default 100 threads)
    #
    def initialize(options={})
      @redis = Batsd::Redis.new(options)
      @threadpool = Threadpool.new(options[:threadpool_size] || 100)
      @statistics = {}
      @targets = {}
      @active_targets = {}
      @retentions = options[:retentions].keys
      @flush_interval = @retentions.first
      now = Time.now.to_i
      @last_flushes = @retentions.inject({}){|l, r| l[r] = now; l }
    end
  
    # Handle the key, value, and sample rate specified in the
    # key. Override this in individual handlers to actually 
    # do something useful
    #
    def handle(key, value, sample_rate)
      @threadpool.queue do
        puts "Received #{key} #{value} #{sample_rate}" if ENV["VVERBOSE"]
      end
    end

    # Exposes the threadpool used by the handler
    #
    def threadpool
      @threadpool
    end

    # Provide some basic statistics about the handler. The preferred
    # way to augment these is to modify the <code>@statistics</code> 
    # object from subclassed handlers
    #
    def statistics
      {
        threadpool_size: @threadpool.pool,  
        queue_depth: @threadpool.size
      }.merge(@statistics)
    end

    protected

    # Flushes the accumulated data that is pending in
    # <code>@active_targets</code> to Redis.
    #
    def flush_targets(name, flush_start = Time.now.to_i, &block)
      puts "Current threadpool queue for #{name}: #{@threadpool.size}" if ENV["VVERBOSE"]

      n = @active_targets.size
      t = Benchmark.measure do 
        ts = (flush_start - flush_start % @flush_interval)
        
        # Reset the working space
        _targets = @active_targets.dup
        @active_targets = {}

        # Chunk the targets and queue their aggregation and storage
        _targets.dup.each_slice(50) do |slice|
          @threadpool.queue ts, slice do |timestamp, pairs|
            pairs.each do |key, data|
              if ENV["VVERBOSE"] and data.is_a?(Array)
                puts "Storing #{data.size} values to redis for #{key} at #{timestamp}"
              else
                puts "Storing value to redis for #{key} at #{timestamp}"
              end

              yield timestamp, key, data
              @redis.store_for_aggregations key, data
            end
          end
        end
      end

      puts "Flushed #{n} #{name} in #{t.real} seconds" if ENV["VERBOSE"]      
    end

    # Write the latter aggregations from redis to an aggregate entry.
    # It does this by tracking the last time they were written.
    # If that was a sufficient time ago, the value will be retrieved
    # from redis, cleared, and written to an aggregate in another thread.
    #
    # When the last level of aggregation (least granularity) is written,
    # the <code>@targets</code> will be flushed to the 'datapoints' set in
    # redis and reset
    #
    def retain_targets(name, flush_start = Time.now.to_i, &block)
      # If it's time for the latter aggregations to be written, queue those up
      @retentions.each_with_index do |retention, index|
        # First retention is always just flushed to redis on the flush interval in +flush_targets+
        next if index.zero?

        # Only if we're in need of a write - if the next flush will be past the threshold
        if (flush_start + @flush_interval) > @last_flushes[retention] + retention.to_i
          puts "Starting writing #{name}@#{retention}" if ENV["VERBOSE"]
          t = Benchmark.measure do 
            ts = (flush_start - flush_start % retention.to_i)
            @targets.dup.keys.each_slice(400) do |keys|
              @threadpool.queue ts, keys, retention do |timestamp, keys, retention|
                keys.each do |key|
                  values = @redis.pop_for_aggregations(key, retention)
                  if values
                    count = values.count
                    puts "Writing the aggregates for #{count} values for #{key} at the #{retention} level" if ENV["VVERBOSE"]
                    yield timestamp, key, retention, values, count
                  end
                end
              end
            end
            @last_flushes[retention] = flush_start
          end
          puts "#{Time.now}: Handled writing #{name}@#{retention} in #{t.real}" if ENV["VERBOSE"]

          # If this is the last retention we're handling, flush the target list to redis and reset it
          if retention == @retentions.last
            puts "Clearing the #{name} list. Current state is: #{@targets}" if ENV["VVERBOSE"]
            t = Benchmark.measure do 
              @redis.add_datapoint @targets.keys
              @targets = {}
            end
            puts "#{Time.now}: Flushed datapoints for #{name} in #{t.real}" if ENV["VERBOSE"]
          end
        end
      end      
    end
  end
end
