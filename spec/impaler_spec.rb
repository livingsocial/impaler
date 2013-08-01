require 'impaler'

# These tests cover the internals of the Impaler library in isolation as much as possible

describe Impaler do
  before(:each) do
    RBHive::Connection.any_instance.stub(:open).and_return(true)
    Impala::Connection.any_instance.stub(:open).and_return(true)
    Impala::Connection.any_instance.stub(:refresh).and_return(true)
  end

  describe "connect" do
    it "fails with no servers" do
      expect { Impaler::connect(nil, nil) }.to raise_error(Impaler::ConnectionError)
    end

    it "handles valid server strings" do

      impala_server = "impala"
      impala_port = "123"
      hivethrift_server = "hivethrift"
      hivethrift_port = "456"

      verify = lambda{|conn, check_impala, check_hivethrift| 
        if check_impala 
          conn.instance_variable_get(:@impala_host).should eq(impala_server)
          conn.instance_variable_get(:@impala_port).should eq(impala_port)
        end
        if check_hivethrift 
          conn.instance_variable_get(:@hivethrift_host).should eq(hivethrift_server)
          conn.instance_variable_get(:@hivethrift_port).should eq(hivethrift_port)
        end
      }

      #Impaler::Manager.any_instance.stub(:open).and_return(true)
      impala = "#{impala_server}:#{impala_port}"
      hivethrift = "#{hivethrift_server}:#{hivethrift_port}"

      verify.call( Impaler.connect( impala, nil ), true, false)
      verify.call( Impaler.connect( nil, hivethrift ), false, true)
      verify.call( Impaler.connect( impala, hivethrift ), true, true)
    end

  end

  describe "handles errors" do
     let(:conn) { Impaler.connect("impala:999","hive:999") }
     let(:impala_conn) { Impaler.connect("impala:999",nil) }
     let(:hive_conn) { Impaler.connect(nil,"hive:999") }
     let(:q) { "select count(*) from foo" }
     let(:q_return) { [{:c=>1}] }

     it "handles the no error state and uses impala" do
       Impala::Connection.any_instance.stub(:query).and_return(q_return)
       RBHive::Connection.any_instance.stub(:fetch).and_raise(StandardError)
       conn.query(q).should eq(q_return)
     end

     it "handles an impala error and falls back to hive" do
       Impala::Connection.any_instance.stub(:query).and_raise(StandardError)
       RBHive::Connection.any_instance.stub(:fetch).and_return(q_return)
       conn.query(q).should eq(q_return)
     end

     it "failure of both throws an error" do
       Impala::Connection.any_instance.stub(:query).and_raise(StandardError)
       RBHive::Connection.any_instance.stub(:fetch).and_raise(StandardError)
       expect { conn.query(q) }.to raise_error(StandardError)
     end

     it "impala only throws an error if impala errs" do
       Impala::Connection.any_instance.stub(:query).and_raise(StandardError)
       RBHive::Connection.any_instance.stub(:fetch).and_return(q_return)
       expect { impala_conn.query(q) }.to raise_error(StandardError)
     end

     it "skipping impala works" do
       Impala::Connection.any_instance.stub(:query).and_return([])
       RBHive::Connection.any_instance.stub(:fetch).and_return(q_return)
       conn.query(q, Impaler::HIVE_ONLY).should eq(q_return)
     end

     it "skipping hive works" do
       Impala::Connection.any_instance.stub(:query).and_raise(StandardError)
       RBHive::Connection.any_instance.stub(:fetch).and_return(q_return)
       expect { conn.query(q, Impaler::IMPALA_ONLY) }.to raise_error(StandardError)
     end
  end


end
