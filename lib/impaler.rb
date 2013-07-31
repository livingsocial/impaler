require "impaler/version"

require "impaler/manager"

module Impaler

  class ConnectionError < StandardError; end

  # Connect to an Impala server. If a block is given, it will close the
  # connection after yielding the connection to the block.
  # @param [String] host the hostname or IP address of the Impala server
  # @param [int] port the port that the Impala server is listening on
  # @yieldparam [Connection] conn the open connection. Will be closed once the block
  #    finishes
  # @return [Connection] the open connection, or, if a block is
  #    passed, the return value of the block
  def self.connect(impala_servers=nil, hivethrift_servers=nil, logger=Logger.new(STDOUT))
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
