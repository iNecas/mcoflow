module Mcoflow
  module Actions
    module Package
      class Install < Mcoflow::Action

        input_format do
          param :hostname, String
          param :package, String
        end

        def mco_agent
          :package
        end

        def mco_action
          :install
        end

        def mco_args
          { :package => input[:package] }
        end

        def update_progress(payload)
          super
          if output[:payload] &&
                output[:payload][:body] &&
                output[:payload][:body][:statuscode] == 0
            output[:run] = true
          else
            output[:run] = false
          end
        end

      end
    end
  end
end
