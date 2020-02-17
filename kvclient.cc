/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>
#include <iomanip>
#include <ctime>
#include <chrono>
#include <map>
#include <fstream>
#include <vector>
#include <sstream>
#include <pthread.h>
#include <unistd.h>

#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using helloworld::HelloRequest;
using helloworld::HelloReply;
using helloworld::Greeter;
using helloworld::setMessageRequest;
using helloworld::setMessageReply;
using helloworld::getMessageRequest;
using helloworld::getMessageReply;
using helloworld::getPrefixRequest;
using helloworld::getPrefixReply;
using namespace std;
using namespace std::chrono;

class GreeterClient {
 public:
  GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string SayHello(const std::string& user) {
    // Data we are sending to the server.
    HelloRequest request;
    request.set_name(user);
    // Container for the data we expect from the server.
    HelloReply reply;
    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(5000);
    context.set_deadline(deadline);
    // The actual RPC.
    Status status = stub_->SayHello(&context, request, &reply);
    // Act upon its status.
    if (status.ok()) {
      return reply.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  std::string SayHelloAgain(const std::string& user) {
    // Follows the same pattern as SayHello.
    HelloRequest request;
    request.set_name(user);
    HelloReply reply;
    ClientContext context;
    // Here we can use the stub's newly available method we just added.
    Status status = stub_->SayHelloAgain(&context, request, &reply);
    if (status.ok()) {
      return reply.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  std::string setValue(const std::string& key, const std::string& value) {
    setMessageRequest request;
    request.set_key(key);
    request.set_value(value);
    setMessageReply reply;
    ClientContext context;

    Status status = stub_->setValue(&context, request, &reply);
    if(status.ok()) {
      return "{" + reply.key() + " - " + reply.value() + "} entered successfully";
    } else {
      std::cout << status.error_code() <<" : "<< status.error_message() << std::endl;
    }
  }

  std::string getValue(const std::string& key) {
    getMessageRequest request;
    request.set_key(key);
    getMessageReply reply;
    ClientContext context;

    Status status = stub_->getValue(&context, request, &reply);
    if(status.ok()) {
      return "{" + reply.key() + " - " + reply.value() + "} -> retreived";
    } else {
      std::cout << status.error_code() <<" : "<< status.error_message() << std::endl;
    }
  }

  std::string getPrefix(const std::string& prefix) {
    getPrefixRequest request;
    request.set_prefix(prefix);
    getPrefixReply reply;
    ClientContext context;

    Status status = stub_->getPrefix(&context, request, &reply);
    if(status.ok()) {
      return "Received " + reply.response();
    } else {
      std::cout << status.error_code() <<" : "<< status.error_message() << std::endl;
    }
  }

 private:
  std::unique_ptr<Greeter::Stub> stub_;
};

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  auto start = high_resolution_clock::now();
  GreeterClient greeter(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()));
  std::string key;
  std::string value;
  int choice;
  std::fstream tempFile;
  tempFile.open(argv[1], std::ios::in);
  if(tempFile) {
     std::string valueStr = "";
     while(tempFile) {
         sleep(1);
         getline(tempFile, valueStr);
         std::stringstream ss(valueStr);
         std::istream_iterator<std::string> begin(ss);
         std::istream_iterator<std::string> end;
         std::vector<std::string> opr(begin, end);
         //std::cout<<opr[0]<<" "<<opr[1]<<std::endl;
         if(opr.size() <= 1)
             break;
         if(opr[0] == "PUT")
         {
           choice = 1;
           key = opr[1];
           //std::cout<<"PUT "<< key <<std::endl;
           value = opr[2];

         }
         else if(opr[0]=="GET")
         {
           choice = 2;
           key = opr[1];
         }
         else if(opr[0]=="PREFIX")
         {
           choice = 3;
           key = opr[1];
         }
         std::string reply;
         std::string user("world");
         switch (choice) {
           case 1:

              reply = greeter.setValue(key, value);
              break;
           case 2:
              reply = greeter.getValue(key);
              //std::cout<<"Reply from server "<< reply <<std::endl;
              break;
           case 3:
              reply = greeter.getPrefix(key);
              //std::cout << "Reply from server " << reply << std::endl;
              break;
           default:
              std::cout<<"Wrong choice entered!"<<std::endl;
         }
     }
  }
  auto stop = high_resolution_clock::now();
  auto duration = duration_cast<microseconds>(stop - start);
  //std::cout << "Time taken by " << argv[1]<<" "<< duration.count() << " microseconds" << std::endl;
  return 0;
}
