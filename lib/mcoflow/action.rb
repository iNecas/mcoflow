module Mcoflow
  class Action < Dynflow::Action
    # what agent should be used for the action
    def mco_agent
      raise RuntimeError, "Not implemented"
    end

    # what mco action should be triggered
    def mco_action
      raise RuntimeError, "Not implemented"
    end

    # what args shlould be passed
    def mco_args
      raise RuntimeError, "Not implemented"
    end

    def mco_filter
      raise "input[:hostname] is expected" unless input[:hostname]
      { identity_filter: input[:hostname] }
    end

    def run(event = nil)
      if event
        update_progress(event[:payload])
      else
        initiate
      end
    end

    def initiate
      suspend do |suspended_action|
        output[:request_id] = Mcoflow.connector.mco_run(suspended_action,
                                                        mco_filter,
                                                        mco_agent,
                                                        mco_action,
                                                        mco_args)
      end
    end

    def update_progress(payload)
      output.update(payload: payload)
    end

  end
end
