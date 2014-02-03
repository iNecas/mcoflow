module Mcoflow
  module Actions
    module Service
      class Restart < Mcoflow::Action

        input_format do
          param :service, String
          param :installations, array_of(Package::Install.output_format)
        end

        def mco_agent
          :service
        end

        def mco_action
          :restart
        end

        def mco_args
          { :service => input[:service] }
        end

        def initiate
          if input[:installations].any?
            restart = input[:installations].any? do |installation|
              installation[:run]
            end
          else
            restart = true
          end
          if restart
            super
          end
        end

      end
    end
  end
end
