module Mcoflow
  module Connectors
    class MCollective < Dynflow::MicroActor

      include ::MCollective::RPC

      REPLY_QUEUE = '/queue/mcollective.mcoflow'

      Task = Algebrick.type do
        fields(suspended_action: Dynflow::Action::Suspended,
               mco_filter: Hash,
               mco_agent: Object,
               mco_action: Object,
               mco_args: Hash)
      end

      Response = Algebrick.type do
        fields(request_id:   String,
               payload:      Hash)
      end

      Event = Algebrick.type do
        fields(done: Object,
               payload: Object)
      end

      def initialize(logger)
        super(logger)
        @actions           =  {}

        stomp_connection.subscribe(REPLY_QUEUE, { :id => 1 })

        @listener = Thread.new do
          loop do
            begin
              raw_msg = stomp_connection.receive
              message  = ::MCollective::Message.new(raw_msg.body, raw_msg)
              message.type = :reply
              message.decode!
              self << Response[message.requestid, message.payload]
            rescue => e
              puts "Error #{e.message}"
              puts e.backtrace.join("\n")
            end
          end
        end
      end

      def mco_run(suspended_action, mco_filter, mco_agent, mco_action, mco_args)
        task = Task[suspended_action, mco_filter, mco_agent, mco_action, mco_args]
        self.ask(task).value!
      end

      private

      def load_stomp_config
        ::MCollective::Config.instance.pluginconf.reduce({}) do |h, (k, v)|
          (key = k[/^activemq.*\.(\w+)$/,1]) ? h.update(key => v) : h
        end
      end

      def stomp_connection
        return @connection if @connection
        stomp_config = load_stomp_config
        config = {:hosts => [{ :login => stomp_config["user"],
                               :passcode => stomp_config["password"],
                               :host => stomp_config["host"],
                               :port => stomp_config["port"] }]}
        @connection = Stomp::Connection.open(config)
      end

      def on_message(message)
        case message
        when Task
          trigger_task(message)
        when Response
          resume_action(message)
        end
      end

      def trigger_task(task)
        client = rpcclient(task[:mco_agent].to_s)
        task[:mco_filter].each do |key, value|
          client.send(key, *Array(value))
        end
        client.reply_to = REPLY_QUEUE
        request_id = client.send(task[:mco_action], task[:mco_args])
        # Due to some bug (probably this one http://projects.puppetlabs.com/issues/17384),
        # we can't really diconnect, as the further rpcclient became
        # unusable
        # client.disconnect
        @actions[request_id] = task[:suspended_action]
        request_id
      end

      def resume_action(response)
        action = @actions[response[:request_id]]
        unless action
          raise "we were not able to update a action #{response[:request_id]}"
        end
        action << Event[true, response[:payload]]
        @actions.delete(response[:request_id])
      end

    end

  end
end
