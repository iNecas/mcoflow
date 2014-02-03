require 'dynflow'
require 'mcollective'
require 'logger'

require 'mcoflow/connectors/mcollective'
require 'mcoflow/action'

module Mcoflow

  def self.actions_path
    File.expand_path('../mcoflow/actions', __FILE__)
  end

  def self.initialize_mcollective(configfile)
    if MCollective::Config.instance.configured
      raise 'Mcollective configuration is already initialized'
    else
      MCollective::Config.instance.loadconfig(configfile)
    end
    @connector = Mcoflow::Connectors::MCollective.new(Logger.new($stderr))
  end

  def self.connector
    unless @connector
      raise 'One needs to run intialize_mcollective first'
    end
    @connector
  end

end
