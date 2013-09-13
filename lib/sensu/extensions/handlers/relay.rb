# ExponentialDecayTimer
#
# Implement an exponential backoff timer for reconnecting to metrics
# backends.
class ExponentialDecayTimer
  attr_accessor :reconnect_time

  def initialize
    @reconnect_time = 0
  end

  def get_reconnect_time(max_reconnect_time, connection_attempt_count)
    if @reconnect_time < max_reconnect_time
      seconds = @reconnect_time + (2**(connection_attempt_count - 1))
      seconds = seconds * (0.5 * (1.0 + rand))
      @reconnect_time = if seconds <= max_reconnect_time
                          seconds
                        else
                          max_reconnect_time
                        end
    end
    @reconnect_time
  end
end

module Sensu::Extension
  # Setup some basic error handling and connection management. Climb on top
  # of Sensu logging capability to log error states.
  class RelayConnectionHandler < EM::Connection

    # XXX: These should be runtime configurable.
    MAX_RECONNECT_ATTEMPTS = 10
    MAX_RECONNECT_TIME = 300 # seconds

    attr_accessor :message_queue, :connection_pool
    attr_accessor :name, :host, :port, :connected
    attr_accessor :reconnect_timer

    # ignore :reek:TooManyStatements
    def post_init
      @is_closed = false
      @connection_attempt_count = 0
      @max_reconnect_time = MAX_RECONNECT_TIME
      @comm_inactivity_timeout = 0 # disable inactivity timeout
      @pending_connect_timeout = 30 # seconds
      @reconnect_timer = ExponentialDecayTimer.new
    end

    def connection_completed
      @connected = true
    end

    def close_connection(*args)
      @is_closed = true
      @connected = false
      super(*args)
    end

    def comm_inactivity_timeout
      logger.info("Connection to #{@name} timed out.")
      schedule_reconnect
    end

    def unbind
      @connected = false
      unless @is_closed
        logger.info('Connection closed unintentionally.')
        schedule_reconnect
      end
    end

    def send_data(*args)
      super(*args)
    end

    # Reconnect normally attempts to connect at the end of the tick
    # Delay the reconnect for some seconds.
    def reconnect(time)
      EM.add_timer(time) do
        logger.info("Attempting to reconnect relay channel: #{@name}.")
        super(@host, @port)
      end
    end

    def get_reconnect_time
      @reconnect_timer.get_reconnect_time(
        @max_reconnect_time,
        @connection_attempt_count
      )
    end

    def schedule_reconnect
      unless @connected
        @connection_attempt_count += 1
        reconnect_time = get_reconnect_time
        logger.info("Scheduling reconnect in #{@reconnect_time} seconds for relay channel: #{@name}.")
        reconnect(reconnect_time)
      end
      reconnect_time
    end

    def logger
      Sensu::Logger.get
    end

  end # RelayConnectionHandler

  # EndPoint
  #
  # An endpoint is a backend metric store. This is a compositional object
  # to help keep the rest of the code sane.
  class Endpoint

    # EM::Connection.send_data batches network connection writes in 16KB
    # We should start out by having all data in the queue flush in the
    # space of a single loop tick.
    MAX_QUEUE_SIZE = 16384

    attr_accessor :connection, :queue

    def initialize(name, host, port)
      @queue = []
      @connection = EM.connect(host, port, RelayConnectionHandler)
      @connection.name = name
      @connection.host = host
      @connection.port = port
      @connection.message_queue = @queue
      EventMachine::PeriodicTimer.new(60) do
        Sensu::Logger.get.info("relay queue size for #{name}: #{queue_length}")
      end
    end

    def queue_length
      @queue
        .map(&:bytesize)
        .reduce(:+)
    end

    def metric_count
      @queue
        .join
        .split('\n')
        .length
    end

    def flush_to_net
      sent = @connection.send_data(@queue.join)
      @queue = [] if sent > 0
    end

    def relay_event(data)
      if @connection.connected
        @queue << data
        if queue_length >= MAX_QUEUE_SIZE
          flush_to_net
          Sensu::Logger.get.debug('relay.flush_to_net: successfully flushed to network')
        end
      end
    end

    def stop
      if @connection.connected
        flush_to_net
        @connection.close_connection_after_writing
      end
    end

  end

  # MetricEmitter
  #
  # Used for connecting to sensu-client locally to emit metric events
  # Handles arbitrary metrics, formats them in the JSON Metric format
  # Sends a JSON Event to sensu locally containing the metrics
  class MetricEmitter
    attr_accessor :host, :port

    def initialize
      @connection = nil
      @metrics = { }
    end

    def emit
      @connection ||= EM.connect(@host, @port, RelayConnectionHandler)
      @connection.send_data(event)
      @metrics.keys.each do |key|
        zero_metric(key)
      end
    end

    def update_metric(name, value)
      zero_metric(name) unless @metrics.has_key?(name)
      @metrics[name] += value
    end

    def zero_metric(name)
      @metrics[name] = 0
    end

    def get_metric(name)
      @metrics[name]
    end

    def event
      output_metrics = []
      @metrics.each do |metric|
        output_metrics << {
          'name' => metric,
          'value' => @metrics[metric],
          'timestamp' => Time.now.to_i,
          'tags' => {}
        }
      end
      {
        'check' => {
          'name' => 'wizardvan_metrics',
          'handler' => 'relay',
          'status' => 0,
          'type' => 'metric',
          'output_type' => 'json',
          'output' => output_metrics.to_json
        }
      }.to_json
    end

    def stop
      @connection.close_connection_after_writing
    end

  end

  # The Relay handler expects to be called from a mutator that has prepared
  # output of the following format:
  # {
  #   :endpoint => { :name => 'name', :host => '$host', :port => $port },
  #   :metric => 'formatted metric as a string'
  # }
  class Relay < Handler

    def initialize
      super
      @endpoints = { }
      @initialized = false
    end

    # ignore :reek:LongMethod
    def post_init
      @settings[:relay].each do |ep_name, ep_settings|
        next if ep_name == :metrics
        @endpoints[ep_name] = Endpoint.new(
          ep_name,
          ep_settings[:host],
          ep_settings[:port]
        )
      end
      setup_metrics_emitter
    end

    def definition
      {
        type: 'extension',
        name: 'relay',
        mutator: 'metrics',
      }
    end

    def name
      'relay'
    end

    def description
      'Relay metrics via a persistent TCP connection'
    end

    def setup_metrics_emitter
      metrics_settings = @settings[:relay][:metrics]
      @emitter = MetricEmitter.new()
      if metrics_settings[:enabled]
        @emitter.host = metrics_settings[:host]
        @emitter.port = metrics_settings[:port]
        EM.add_periodic_timer(metrics_settings[:interval]) do
          @emitter.emit
        end
      end
    end

    # ignore :reek:LongMethod
    def run(event_data)
      begin
        event_data.keys.each do |ep_name|
          endpoint = @endpoints[ep_name]
          logger.debug("relay.run() handling endpoint: #{ep_name}")
          endpoint.relay_event(event_data[ep_name])
          @emitter.update_metric(
            "#{ep_name}.metrics_sent",
            endpoint.metric_count
          )
        end
      rescue => error
        yield(error.to_s, 2)
      end
      yield('', 0)
    end

    def stop
      @endpoints.each_value do |ep|
        ep.stop
      end
      if @settings[:relay][:metrics][:enabled]
        @emitter.emit
        @emitter.stop
      end
      yield
    end

    def logger
      Sensu::Logger.get
    end

  end # Relay
end # Sensu::Extension
