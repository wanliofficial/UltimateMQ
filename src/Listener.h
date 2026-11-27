#ifndef LISTENER_H
#define LISTENER_H
#pragma once

#include <netinet/in.h>

namespace MQTT {

class Listener {
    int port;
    int sockfd;
    struct sockaddr_in address;
public:
    Listener(int port, bool loopback = false);
    ~Listener();

    void start();
    int acceptConnection();
    //! The listening descriptor (for poll()), or -1 before start()
    int fd() const { return sockfd; }
    void stop();
};

}

#endif