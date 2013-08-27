require "rbhive"
require "impala"

module Impaler

  IMPALA_THEN_HIVE = 1
  HIVE_ONLY = 2
  IMPALA_ONLY = 3

  class Manager

    def initialize(impala_servers, hivethrift_servers, logger=Impaler::DEFAULT_LOGGER)
      if impala_servers.nil? and hivethrift_servers.nil? 
        raise Impaler::ConnectionError.new("No impaler or hive servers were specified, at least one is required")
      end

      if !impala_servers.nil?
        if impala_servers.respond_to?(:choice)
          @impala_servers=impala_servers
        else 
          @impala_servers=[impala_servers]
        end

        impala_server = @impala_servers.choice.split(":")
        @impala_host = impala_server[0]
        @impala_port = impala_server[1]
      end

      if !hivethrift_servers.nil?
        if hivethrift_servers.respond_to?(:choice)
          @hivethrift_servers=hivethrift_servers
        else
          @hivethrift_servers=[hivethrift_servers]
        end
        hivethrift_server = @hivethrift_servers.choice.split(":")
        @hivethrift_host = hivethrift_server[0]
        @hivethrift_port = hivethrift_server[1]
      end

      @logger = logger
      open
    end

    def open
      connected=false
      if !@impala_host.nil? && !@impala_port.nil?
        @logger.debug "Impala connection #{@impala_host}:#{@impala_port}"
        @impala_connection = Impala.connect(@impala_host, @impala_port)
        @impala_connection.open
        @impala_connection.refresh
        connected=true
      else
        @impala_connection = nil
      end

      if !@hivethrift_host.nil? && !@hivethrift_port.nil?
        @logger.debug "Hivethrift connection #{@hivethrift_host}:#{@hivethrift_port}"
        @hivethrift_connection = RBHive::Connection.new(@hivethrift_host, @hivethrift_port)
        @hivethrift_connection.open
        connected=true
      else
        @hivethrift_connection = nil
      end

      if !connected
        raise Impaler::ConnectionError.new("All connections failed")
      end
    end


    # ###########################################################################
    # General use methods

    def query(sql, query_mode = Impaler::IMPALA_THEN_HIVE)
      ret = nil
      error = nil
      success = false
      unless query_mode == Impaler::HIVE_ONLY or @impala_connection.nil? 
        begin
          @logger.debug "Trying query in impala"
          ret = @impala_connection.query(sql)
          @logger.debug "Successful query in impala"
          success = true
        rescue StandardError => e
          error = e
          @logger.warn "Impala error: #{e}"
        end
      end

      unless @hivethrift_connection.nil? || success || query_mode == Impaler::IMPALA_ONLY 
        begin
          @logger.debug "Trying query in hive"
          ret = @hivethrift_connection.fetch(sql)
          @logger.debug "Successful query in hive"
          success = true
        rescue StandardError => e
          error = e
          @logger.warn "Hive error: #{e}"
        end
      end

      if !success && !error.nil? 
        raise error
      elsif !success 
        raise Impaler::QueryError.new("Query did not run due to no connections being available")
      end
      return ret
    end


    # ###########################################################################
    # Helper query methods
    
    def row_count(tablename)
        query("SELECT COUNT(1) c FROM #{tablename}").first[:c]
    end





    # ###########################################################################
    # Metadata methods

    def columns(tablename)
      desc = {}
      (query "describe #{tablename}").each { |col|
        cname=col[:name].nil? ? col[:col_name] : col[:name]
        ctype=col[:type].nil? ? col[:data_type] : col[:type]
        desc[cname.intern] = ctype.intern
      }
      desc
    end




  end


end
