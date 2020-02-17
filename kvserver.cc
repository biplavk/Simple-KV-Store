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
#include <map>
#include <fstream>
#include <pthread.h>
#include <unistd.h>
#include <vector>
#include <sstream>
#include <set>

#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;
using helloworld::HelloRequest;
using helloworld::HelloReply;
using helloworld::Greeter;
using helloworld::setMessageRequest;
using helloworld::setMessageReply;
using helloworld::getMessageRequest;
using helloworld::getMessageReply;
using helloworld::getPrefixRequest;
using helloworld::getPrefixReply;

pthread_mutex_t lock;
// Logic and data behind the server's behavior.
struct arg_struct {
  ServerContext* context;
  const setMessageRequest* request;
  setMessageReply* reply;

};

#define NUM_THREADS 2500000
int threadCounter = 0;
pthread_t thread[NUM_THREADS];
std::map<std::string, std::string> m;
std::fstream keyFile;
int newCounter = 0;
int updateCounter = 0;
int getFoundCounter = 0;
int getNotFoundCounter = 0;
int prefixCounter  =0;
void *_setValue(void *arguments) {
                  pthread_mutex_lock(&lock);
                  struct arg_struct *args = (struct arg_struct *)arguments;
                  std::string key = args->request->key();
                  std::string value = args->request->value();
                  //sleep(5);
                  int newEntry = 0;
                  if(m.count(key)) {
                     updateCounter++;
                    //std::cout<<updateCounter<<std::endl;
                    std::string oldValue = m.at(key);
                    m.erase(key);
                    m.insert({key, value});
                    // check
                    std::fstream kf;
                    std::string checkString;
                    std::string space = " ";
                    std::string endMarker = "END";
                    kf.open("keyFile.txt", std::ios::in | std::ios::out);
                    if(kf) {
                      kf.seekg (0, kf.end);
                      kf << key;
                      kf << space;
                      kf << oldValue;
                      kf << space;
                      kf << endMarker << std::endl;
                      kf.flush();
                      kf.seekg(0, kf.beg);
                      while(getline(kf, checkString)) {
                        std::stringstream ss(checkString);
                        std::istream_iterator<std::string> begin(ss);
                        std::istream_iterator<std::string> end;
                        std::vector<std::string> opr(begin, end);
                        if(opr[0] == key) {
                          int position = kf.tellg();
                          int length = key.length();
                          kf.seekp(position - length - 1);
                          std::string tempEntry(length, '#');
                          kf << tempEntry << std::endl;
                          kf.flush();
                          break;
                        }
                      }
                      kf.close();
                    }
                    //CHECK ENDS
                  } else {
                    newEntry = 1;
                    newCounter++;
                    //std::cout<<"new "<<newCounter<<std::endl;
                    m.insert({key, value});

                  std::fstream file;
                  file.open( key + ".txt",std::ios::trunc | std::ios::out);
                  file<<value;
                  file.flush();
                  file.close();
                  keyFile.open("keyFile.txt",std::ios::out | std::ios::app);
                  keyFile<<key<<std::endl;
                  keyFile.flush();
                  keyFile.close();
                  if(!newEntry) {
                    std::fstream fileRemove;
                    fileRemove.open("keyFile.txt",std::ios::out | std::ios::in);
                    std::string findString;
                    while(getline(fileRemove, findString)) {
                      int totalLength = findString.length();
                      std::stringstream ss(findString);
                      std::istream_iterator<std::string> begin(ss);
                      std::istream_iterator<std::string> end;
                      std::vector<std::string> opr(begin, end);
                      if(opr[0] == key && opr.size() == 3) {
                        int position = fileRemove.tellg();
                        fileRemove.seekp(position - totalLength - 1);
                        std::string tempEntry(totalLength, '#');
                        fileRemove << tempEntry << std::endl;
                        // std::cout<<"Printing #"<<std::endl;
                        fileRemove.flush();
                        break;
                      }
                    }
                    fileRemove.close();
                  }
                }
                  args->reply->set_key(key);
                  args->reply->set_value(value);
                  pthread_mutex_unlock(&lock);
}

class GreeterServiceImpl final : public Greeter::Service {

  Status SayHello(ServerContext* context, const HelloRequest* request,
                  HelloReply* reply) override {
    std::string prefix("Hello ");
    reply->set_message(prefix + request->name());
    return Status::OK;
  }

  Status SayHelloAgain(ServerContext* context, const HelloRequest* request,
                  HelloReply* reply) override {
                    std::string prefix("Hello Again ");
                    reply->set_message(prefix + request->name());
                    return Status::OK;
  }

  Status setValue(ServerContext* context, const setMessageRequest* request,
                  setMessageReply* reply) override {
                  //std::cout<<"Replying..\n";
                  if (context->IsCancelled()) {
                        return Status(StatusCode::CANCELLED, "Deadline exceeded or Client cancelled, abandoning.");
                  }
                  struct arg_struct args;
                  args.context = context;
                  args.request = request;
                  args.reply   = reply;

                  pthread_create(&thread[threadCounter], NULL, &_setValue, (void*)&args);
                  //sleep(1);
                  //std::cout<<pthread_self()<<" "<<threadCounter<<"\n";
                  pthread_join(thread[threadCounter], NULL);
                  threadCounter++;
                  //std::cout<<"PUT:- New Entries are "<<newCounter<<" and updates are "<<updateCounter<<" and getFoundCount is "<<getFoundCounter<<" and getNotFoundCounter is "<<getNotFoundCounter<<std::endl;
                  return Status::OK;
  }


  Status getValue(ServerContext* context, const getMessageRequest* request,
                  getMessageReply* reply) override {
                    if (context->IsCancelled()) {
                          return Status(StatusCode::CANCELLED, "Deadline exceeded or Client cancelled, abandoning.");
                    }
                    std::string key = request->key();
                    if(m.count(key)) {
                      getFoundCounter++;
                      reply->set_value(m.at(key));
                    } else {
                      getNotFoundCounter++;
                      reply->set_value("Key value pair did not exist in map.");
                    }
                    //std::cout<<"GET: New Entries are "<<newCounter<<" and updates are "<<updateCounter<<" and getFoundCount is "<<getFoundCounter<<" and getNotFoundCounter is "<<getNotFoundCounter<<" "<<"Map Size "<<m.size()<<std::endl;
                    reply->set_key(key);
                    return Status::OK;
  }

  Status getPrefix(ServerContext* context, const getPrefixRequest* request,
                  getPrefixReply* reply) override {
		    prefixCounter++;
                    if (context->IsCancelled()) {
                          return Status(StatusCode::CANCELLED, "Deadline exceeded or Client cancelled, abandoning.");
                    }
                    std::string prefix = request->prefix();
                    int length = prefix.size();
                    std::string returnValue;
                    for(auto itr = m.begin(); itr != m.end(); itr++) {
                      std::string key = itr->first;
                      if(key.substr(0, length) == prefix) {
                        returnValue = returnValue + " " + key;
                      }
                    }
                    reply->set_response(returnValue);
                    return Status::OK;
  }

};


void RunServer() {
  std::string server_address("0.0.0.0:50051");
  GreeterServiceImpl service;

  auto start = std::chrono::high_resolution_clock::now();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::map<std::string, std::string> tempMap;
  // std::set<std::string> endSet;
  std::set<std::string> alreadySeen;
  keyFile.open("keyFile.txt",std::ios::in);
  std::string key;
  // Setting up in-memory map
  // new code begins
  std::string currentLine;
  std::string value;
  while(getline(keyFile, currentLine)) {
    //std::cout<<"Read line from file -> "<<currentLine<<std::endl;
    if(currentLine[0] == '#') {
      //std::cout<<"Discarded entry "<<currentLine<<std::endl;
      continue;
    } else {
      std::stringstream ss(currentLine);
      std::istream_iterator<std::string> begin(ss);
      std::istream_iterator<std::string> end;
      std::vector<std::string> opr(begin, end);
      if(opr.size() == 1 || opr.size() == 2) {
        alreadySeen.insert(opr[0]);
        if(tempMap.count(opr[0])) {
          // endSet.erase(opr[0]);
          tempMap.erase(opr[0]);
        }
        std::fstream tempFile;
        tempFile.open(opr[0] + ".txt", std::ios::in);
        if(tempFile) {
          while(tempFile) {
              getline(tempFile, value);
          }
          //std::cout<<"Inserted into map "<< key<< " "<<value<<std::endl;
          m.insert({opr[0], value});
        }
      } else if(opr.size() == 3 && opr[2] == "END") {
        // endSet.insert(opr[0]);
        tempMap.insert({opr[0], opr[1]});
      }
    }
  }
  keyFile.close();
  // clearing tempMap entries
  for(auto itr = tempMap.begin(); itr != tempMap.end(); itr++) {
    std::string k = itr->first;
    std::string v = itr->second;
    std::fstream file;
    file.open( k + ".txt",std::ios::trunc | std::ios::out);
    file<<v;
    file.flush();
    file.close();
    m.insert({k,v});
  }
  tempMap.clear();

  // keyFile cleanup
  std::vector<std::string> toBeInserted;
  keyFile.open("keyFile.txt",std::ios::in | std::ios::out);
  std::string lineRead;
  while(getline(keyFile, lineRead)) {
    if(lineRead[0] == '#') {
      continue;
    } else {
      std::stringstream ss(lineRead);
      std::istream_iterator<std::string> begin(ss);
      std::istream_iterator<std::string> end;
      std::vector<std::string> opr(begin, end);
      if(opr.size() == 1) {
        continue;
      } else if(opr.size() == 2) {
        std::string k = opr[0];
        int position = keyFile.tellg();
        int length = lineRead.length();
        keyFile.seekp(position - length - 1);
        std::string tempEntry(length, '#');
        keyFile << tempEntry << std::endl;
        keyFile.flush();
        toBeInserted.push_back(k);
      } else if(opr.size() == 3) {
        std::string k = opr[0];
        // if(endSet.count(k)) {
        toBeInserted.push_back(k);
        // }
        int position = keyFile.tellg();
        int length = lineRead.length();
        keyFile.seekp(position - length - 1);
        std::string tempEntry(length, '#');
        keyFile << tempEntry << std::endl;
        keyFile.flush();
      }
    }
  }
  keyFile.close();
  keyFile.open("keyFile.txt",std::ios::out | std::ios::app);
  for(std::string s : toBeInserted) {
    if(alreadySeen.count(s) == 0) {
      keyFile<<s<<std::endl;
    }
  }
  alreadySeen.clear();
  keyFile.close();

  // new code ends
  // OLD CODE BEGINS
  // while (std::getline(keyFile, key)){
  //     std::cout<<"Read key value from keyFile:- "<<key<<std::endl;
  //     if(key[0] == '#') {
  //       std::cout<<"Discarded entry "<<key<<std::endl;
  //       continue;
  //     }
  //     std::fstream tempFile;
  //     tempFile.open(key + ".txt", std::ios::in);
  //     std::string value;
  //     if(tempFile) {
  //       while(tempFile) {
  //           getline(tempFile, value);
  //       }
  //         std::cout<<"Inserted into map "<< key<< " "<<value<<std::endl;
  //         m.insert({key, value});
  //     } else {
  //       std::cout<<"File data corrupted. Please check file system"<<std::endl;
  //     }
  // }
  // keyFile.close();
  // OLD CODE ENDS
  auto finish = std::chrono::high_resolution_clock::now();
  std::cout << "Server Startup Time "<<std::chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count() <<" ns"<< std::endl;
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  if (pthread_mutex_init(&lock, 0) != 0) {
      std::cout<<"mutex init has failed\n";
      return 1;
  }
  RunServer();

  pthread_mutex_destroy(&lock);

  return 0;
}
