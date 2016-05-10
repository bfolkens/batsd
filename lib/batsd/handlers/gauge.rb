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
      super
    end

    # Process an incoming gauge measurement
    #
    # * Normalize for sample rate provided
    # * Write current timestamp and value
    # * Store the name of the datapoint in Redis 
    #
    def handle(key, value, sample_rate)
      @threadpool.queue Time.now.to_i, key, value, sample_rate do |timestamp, key, value, sample_rate|
        puts "Received #{key} #{value} #{sample_rate}" if ENV["VVERBOSE"]
        if sample_rate
          value = value.to_f / sample_rate.gsub("@", "").to_f
        end
        key = "gauges:#{key}"
        @redis.store_value timestamp, key, value
        @redis.add_datapoint key
      end
    end

  end
end

