require 'sinatra'
require 'mcollective'
require 'pry'

$:.unshift(File.expand_path('../lib', __FILE__))
require 'mcoflow'

Dir.glob(File.join(Mcoflow.actions_path, "**/*.rb")) { |f| require f }

Mcoflow.initialize_mcollective(File.expand_path('~/.mcollective'))

dynflow_config = {}
dynflow_config[:persistence_adapter] =  Dynflow::PersistenceAdapters::Sequel.new('sqlite://db/mcoflow.sqlite')
WORLD = Dynflow::SimpleWorld.new(dynflow_config)

helpers ERB::Util

get '/' do
  @uuid = params[:uuid]
  erb :index
end

post '/trigger' do
  hostname = params[:hostname]
  raise "hostname can't be empty" if hostname.nil? || hostname.empty?
  options = { hostname: hostname }
  task = case params[:action].to_s.downcase
            when "install"
              WORLD.trigger(Mcoflow::Actions::Package::Install,
                            options.merge(package: params[:param]))
            when "uninstall"
              WORLD.trigger(Mcoflow::Actions::Package::Uninstall,
                            options.merge(package: params[:param]))
            when "restart"
              WORLD.trigger(Mcoflow::Actions::Service::Restart,
                            options.merge(service: params[:param]))
            when "install_and_restart"
              packages, services = params[:param].split(';')
              packages = packages.to_s.split(',').map(&:strip)
              services = services.to_s.split(',').map(&:strip)
              WORLD.trigger(Mcoflow::Actions::InstallAndRestart,
                            hostname, packages, services)
            else
              raise "unkown action #{params[:action]}"
         end
  redirect "/?uuid=#{task.id}"
end
