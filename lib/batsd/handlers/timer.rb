module Batsd
  #
  # Handles timer measurements ("|c")
  #
  # Timer measurements are aggregated in various ways across 
  # aggregation intervals
  #
  class Handler::Timer < Handler

    # Set up a new handler to handle timers
    #
    # * Set up a redis client
    # * Initialize last flush timers to now
    #
    def initialize(options)
      @redis = Batsd::Redis.new(options)
      @retentions = options[:retentions].keys
      @flush_interval = @retentions.first
      @active_timers = {}
      @timers = {}
      now = Time.now.to_i
      @last_flushes = @retentions.inject({}){|l, r| l[r] = now; l }
      @fast_threadpool = Threadpool.new((options[:threadpool_size] || 100)/2)
      super
    end

    # Handle the key, value, and sample rate for a timer
    #
    # Store timers in a hashed array (<code>{a: [], b:[]}</code>) and
    # the set of timers we know about in a hash of nil values
    def handle(key, value, sample_rate)
      key = "timers:#{key}"
      if value
        @active_timers[key] ||= []
        @active_timers[key].push value.to_f
        @timers[key] = nil
      end
    end

    # Flush timers to redis.
    #
    # 1) At every flush interval, flush to redis and clear active timers. Also
    #    store raw values for usage later.
    # 2) If time since last write for a given aggregation, flush.
    # 3) If flushing the terminal aggregation, flush the set of datapoints to
    #    Redis and reset that tracking in process.
    #
    def flush
      puts "Current threadpool queue for timers: #{@threadpool.size}" if ENV["VVERBOSE"]
      # Flushing is usually very fast, but always fix it so that the
      # entire thing is based on a constant start time
      # Saves on time syscalls too
      flush_start = Time.now.to_i
      
      n = @active_timers.size
      t = Benchmark.measure do 
        ts = (flush_start - flush_start % @flush_interval)
        timers = @active_timers.dup
        @active_timers = {}
        timers.each_slice(50) do |keys|
          @fast_threadpool.queue ts, keys do |timestamp, keys|
            keys.each do |key, values|
              puts "Storing #{values.size} values to redis for #{key} at #{timestamp}" if ENV["VVERBOSE"]
              # Store all the aggregates for the flush interval level
              count = values.count
              @redis.store_value timestamp, "#{key}:mean", values.mean
              @redis.store_value timestamp, "#{key}:count", count 
              @redis.store_value timestamp, "#{key}:min", values.min
              @redis.store_value timestamp, "#{key}:max", values.max
              @redis.store_value timestamp, "#{key}:upper_90", values.percentile_90
              if count > 1
                @redis.store_value timestamp, "#{key}:stddev", values.standard_dev
              end
              @redis.store_for_aggregations key, values
            end
          end
        end
      end
      puts "Flushed #{n} timers in #{t.real} seconds" if ENV["VERBOSE"]

      # If it's time for the latter aggregations to be written, queue those up
      @retentions.each_with_index do |retention, index|
        # First retention is always just flushed to redis on the flush interval
        next if index.zero?

        # Only if we're in need of a write - if the next flush will be
        # past the threshold
        if (flush_start + @flush_interval) > @last_flushes[retention] + retention.to_i
          puts "Starting writing timers@#{retention}" if ENV["VERBOSE"]
          t = Benchmark.measure do 
            ts = (flush_start - flush_start % retention.to_i)
            @timers.dup.keys.each_slice(400) do |keys|
              @threadpool.queue ts, keys, retention do |timestamp, keys, retention|
                keys.each do |key|
                  values = @redis.pop_for_aggregations(key, retention)
                  if values
                    values = values.collect(&:to_f)
                    count = values.count
                    puts "Writing the aggregates for #{count} values for #{key} at the #{retention} level." if ENV["VVERBOSE"]
                    ["mean", "count", "min", "max", ["upper_90", "percentile_90"], ["stddev", "standard_dev"]].each do |aggregation|
                      if aggregation.is_a? Array
                        name = aggregation[0]
                        aggregation = aggregation[1]
                      else
                        name = aggregation
                      end
                      val = (count > 1 ? values.send(aggregation.to_sym) : values.first)
                      @redis.store_value timestamp, "#{key}:#{name}:#{retention}", val
                    end
                  end
                end
              end
            end
            @last_flushes[retention] = flush_start
          end
          puts "#{Time.now}: Handled writing timers@#{retention} in #{t.real}" if ENV["VERBOSE"]

          # If this is the last retention we're handling, flush the
          # times list to redis and reset it
          if retention == @retentions.last
            puts "Clearing the timers list. Current state is: #{@timers}" if ENV["VVERBOSE"]
            t = Benchmark.measure do 
              @redis.add_datapoint @timers.keys
              @timers = {}
            end
            puts "#{Time.now}: Flushed datapoints for timers in #{t.real}" if ENV["VERBOSE"]
          end

        end
      end

    end


  end
end
