module Batsd
  #
  # Handles counter measurements ("|c")
  #
  # Counter measurements are summed together across 
  # aggregation intervals
  #
  class Handler::Counter < Handler

    # Processes an incoming counter measurement
    #
    # * Normalize for sample rate provided
    # * Adds the value to any existing values by the same
    #   key and stores it in <code>@active_targets</code>
    # * Add the key and a nil value to <code>@targets</code>
    #   in order to track the set of counters that have been
    #   handled "recently". This is a relatively memory efficient,
    #   relatively fast way of storing a unique set of keys.
    #
    def handle(key, value, sample_rate)
      if sample_rate
        value = value.to_f / sample_rate.gsub("@", "").to_f
      end
      key = "counters:#{key}"
      @active_targets[key] = @active_targets[key].to_i + value.to_i
      @targets[key] = nil
    end

    def flush
      flush_start = Time.now.to_i

      flush_targets(:counters, flush_start) do |timestamp, key, value|
        @redis.store_value timestamp, key, value
      end
      
      retain_targets(:counters, flush_start) do |timestamp, key, retention, values, count|
        values = values.collect(&:to_f)
        val = (count > 1 ? values.sum : values.first)
        @redis.store_value timestamp, "#{key}:#{retention}", val
      end
    end

  end
end
