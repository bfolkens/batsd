module Batsd
  #
  # Handles gauge measurements ("|g")
  #
  # Gauge measurements are corrected for sample rate
  # (or more accurately, scale), if provided and written 
  # to redis without any manipulation or collection.
  #
  class Handler::Gauge < Handler
    
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
      @active_targets[key] = value
      @targets[key] = nil
    end

    def flush
      flush_start = Time.now.to_i

      flush_targets(:gauges, flush_start) do |timestamp, key, value|
        @redis.store_value timestamp, key, value
      end

      retain_targets(:gauges, flush_start) do |timestamp, key, retention, values, count|
        values = values.collect(&:to_f)
        val = (count > 1 ? values.mean : values.first)
        @redis.store_value timestamp, "#{key}:#{retention}", val
      end
    end

  end
end

