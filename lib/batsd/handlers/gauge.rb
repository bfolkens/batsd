module Batsd
  #
  # Handles gauge measurements ("|g")
  #
  # Gauge measurements are corrected for sample rate
  # (or more accurately, scale), if provided and written 
  # to redis without any manipulation or collection.
  #
  class Handler::Gauge < Handler
    
    # Set up a new handler to handle gauges
    #
    # * Set up a redis client
    #
    def initialize(options)
      @redis = Batsd::Redis.new(options)
      @gauges = @active_gauges = {}
      @retentions = options[:retentions].keys
      @flush_interval = @retentions.first
      now = Time.now.to_i
      @last_flushes = @retentions.inject({}){|l, r| l[r] = now; l }
      super
    end

    # Process an incoming gauge measurement
    #
    # * Normalize for sample rate provided
    # * Write current timestamp and value
    # * Store the name of the datapoint in Redis 
    #
    def handle(key, value, sample_rate)
      if sample_rate
        value = value.to_f / sample_rate.gsub("@", "").to_f
      end
      key = "gauges:#{key}"
      @gauges[key] = value
    end

    def flush
      puts "Current threadpool queue for gauges: #{@threadpool.size}" if ENV["VVERBOSE"]
      # Flushing is usually very fast, but always fix it so that the
      # entire thing is based on a constant start time
      # Saves on time syscalls too
      flush_start = Time.now.to_i

      n = @gauges.size
      t = Benchmark.measure do 
        ts = (flush_start - flush_start % @flush_interval)
        @gauges.dup.each_slice(50) do |keys|
          @threadpool.queue ts, keys do |timestamp, keys|
            keys.each do |key, value|
              @redis.store_value timestamp, key, value
              @redis.store_for_aggregations key, value
            end
          end
        end
      end
      puts "Flushed #{n} gauges in #{t.real} seconds" if ENV["VERBOSE"]

      # If it's time for the latter aggregation to be written, queue those up
      @retentions.each_with_index do |retention, index|
        # First retention is always just flushed to redis on the flush interval
        next if index.zero?

        # Only if we're in need of a write - if the next flush will be
        # past the threshold
        if (flush_start + @flush_interval) > @last_flushes[retention] + retention.to_i
          puts "Starting writing for gauges@#{retention}" if ENV["VERBOSE"]
          t = Benchmark.measure do 
            ts = (flush_start - flush_start % retention.to_i)
            @gauges.keys.each_slice(400) do |keys|
              @threadpool.queue ts, keys, retention do |timestamp, keys, retention|
                keys.each do |key|
                  values = @redis.pop_for_aggregations(key, retention)
                  if values
                    values = values.collect(&:to_f)
                    count = values.count
                    puts "Writing the aggregates for #{count} values for #{key} at the #{retention} level." if ENV["VVERBOSE"]
                    val = (count > 1 ? values.mean : values.first)
                    @redis.store_value timestamp, "#{key}:#{retention}", val
                  end
                end
              end
            end
            @last_flushes[retention] = flush_start
          end
          puts "#{Time.now}: Handled writing for gauges@#{retention} in #{t.real}" if ENV["VERBOSE"]

          # If this is the last retention we're handling, flush the
          # gauges list to redis and reset it
          if retention == @retentions.last
            puts "Clearing the gauges list. Current state is: #{@gauges}" if ENV["VVERBOSE"]
            t = Benchmark.measure do 
              @redis.add_datapoint @gauges.keys
            end
            puts "#{Time.now}: Flushed datapoints for gauges in #{t.real}" if ENV["VERBOSE"]
          end
        end

      end

    end

  end
end

