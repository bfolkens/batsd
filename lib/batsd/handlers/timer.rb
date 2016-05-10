module Batsd
  #
  # Handles timer measurements ("|c")
  #
  # Timer measurements are aggregated in various ways across 
  # aggregation intervals
  #
  class Handler::Timer < Handler

    # Handle the key, value, and sample rate for a timer
    #
    # Store timers in a hashed array (<code>{a: [], b:[]}</code>) and
    # the set of timers we know about in a hash of nil values
    #
    def handle(key, value, sample_rate)
      key = "timers:#{key}"
      if value
        @active_targets[key] ||= []
        @active_targets[key].push value.to_f
        @targets[key] = nil
      end
    end

    def flush
      flush_start = Time.now.to_i
      
      flush_targets(:timers, flush_start) do |timestamp, key, values|
        count = values.count
        @redis.store_value timestamp, "#{key}:mean", values.mean
        @redis.store_value timestamp, "#{key}:count", count 
        @redis.store_value timestamp, "#{key}:min", values.min
        @redis.store_value timestamp, "#{key}:max", values.max
        @redis.store_value timestamp, "#{key}:upper_90", values.percentile_90
        if count > 1
          @redis.store_value timestamp, "#{key}:stddev", values.standard_dev
        end
      end

      retain_targets(:timers, flush_start) do |timestamp, key, retention, values|
        values = values.collect(&:to_f)
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
