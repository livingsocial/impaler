require "impaler/version"

require "impaler/manager"

module Impaler

  class ConnectionError < StandardError; end

  # Connect to the servers and optionally execute a block of code
  # with the servers.
  # @param [String] host:port for the impala server or an array of host:port to pick from many
  # @param [String] host:port for the hive thirft server (v1) or an array of host:port to pick from many
  # @yieldparam [Connection] conn the open connection. Will be closed once the block
  #    finishes
  # @return [Connection] the open connection, or, if a block is
  #    passed, the return value of the block
  def self.connect(impala_servers, hivethrift_servers, logger=Logger.new(STDOUT))
    manager = Manager.new(impala_servers, hivethrift_servers, logger=logger)

    if block_given?
      begin
        ret = yield manager
      ensure
        manager.close
      end
    else
      ret = manager
    end

    ret
  end
end
