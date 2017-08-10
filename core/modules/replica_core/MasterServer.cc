/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "replica_core/MasterServer.h"

// System headers

#include <iostream>
#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/DeleteRequest.h"
#include "replica_core/FindRequest.h"
#include "replica_core/FindAllRequest.h"
#include "replica_core/ReplicationRequest.h"
#include "replica_core/ServiceManagementRequest.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/StatusRequest.h"
#include "replica_core/StopRequest.h"

// This macro to appear witin each block which requires thread safety

#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_requestPocessingMtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.MasterServer");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

//////////////////////////////////////////////////////////////////////
//////////////////////////  RequestWrapper  //////////////////////////
//////////////////////////////////////////////////////////////////////

/**
 * The base class for implementing a polymorphic collection of active requests.
 */
struct RequestWrapper {

    /// The pointer type for instances of the class
    typedef std::shared_ptr<RequestWrapper> pointer;

    /// Destructor
    virtual ~RequestWrapper() {}

    /// This method will be called upon a completion of a request
    /// to notify a subscriber on the event.
    virtual void notify ()=0;

    /// Return a pointer to the stored request object
    virtual std::shared_ptr<Request> request () const=0;
};


//////////////////////////////////////////////////////////////////////////
//////////////////////////  RequestWrapperImpl  //////////////////////////
//////////////////////////////////////////////////////////////////////////

/**
 * Request-type specific wrappers
 */
template <class  T>
struct RequestWrapperImpl
    :   RequestWrapper {

    /// The implementation of the vurtual method defined in the base class
    virtual void notify () {
        if (_onFinish == nullptr) return;
        _onFinish(_request);
    }

    RequestWrapperImpl(const typename T::pointer &request,
                       typename T::callback_type  onFinish)

        :   RequestWrapper(),

            _request  (request),
            _onFinish (onFinish) {
    }

    /// Destructor
    virtual ~RequestWrapperImpl() {}

    virtual std::shared_ptr<Request> request () const { return _request; }

private:

    // The context of the operation
    
    typename T::pointer       _request;
    typename T::callback_type _onFinish;
};


////////////////////////////////////////////////////////////////////////
//////////////////////////  MasterServerImpl  //////////////////////////
////////////////////////////////////////////////////////////////////////


/**
 * The utiliy class implementing operations on behalf of certain
 * methods of class MasterServer.
 * 
 * THREAD SAFETY NOTE: Methods implemented witin the class are NOT thread-safe.
 *                     They must be called from the thread-safe code only.
 */
class MasterServerImpl {

public:

    /// Default constructor
    MasterServerImpl () {}

    // Default copy semantics is proxibited

    MasterServerImpl (MasterServerImpl const&) = delete;
    MasterServerImpl & operator= (MasterServerImpl const&) = delete;

    /// Destructor
    ~MasterServerImpl () {}

    /**
     * Generic method for managing requests such as stopping an outstanding
     * request or inquering a status of a request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be affected
     * @param onFinish        - a callback function to be called upon completion of the operation
     */
    template <class REQUEST_TYPE>
    static
    typename REQUEST_TYPE::pointer requestManagementOperation (
            MasterServer::pointer                 server, 
            const std::string                    &workerName,
            const std::string                    &targetRequestId,
            typename REQUEST_TYPE::callback_type  onFinish) {

        server->assertIsRunning();

        typename REQUEST_TYPE::pointer request =
            REQUEST_TYPE::create (
                server->_serviceProvider,
                workerName,
                server->_io_service,
                targetRequestId,
                [server] (typename REQUEST_TYPE::pointer request) {
                    server->finish(request->id());
                }
            );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.
    
        (server->_registry)[request->id()] =
            std::make_shared<RequestWrapperImpl<REQUEST_TYPE>> (request, onFinish);  
    
        // Initiate the request

        request->start ();

        return request;
    }

   /**
     * Generic method for launching worker service management requests such as suspending,
     * resyming or inspecting a status of the worker-side replication service.
     *
     * @param workerName - the name of a worker node where the service is run
     * @param onFinish   - a callback function to be called upon completion of the operation
     */
    template <class REQUEST_TYPE>
    static
    typename REQUEST_TYPE::pointer serviceManagementOperation (
            MasterServer::pointer                 server, 
            const std::string                    &workerName,
            typename REQUEST_TYPE::callback_type  onFinish) {

        server->assertIsRunning();

        typename REQUEST_TYPE::pointer request =
            REQUEST_TYPE::create (
                server->_serviceProvider,
                workerName,
                server->_io_service,
                [server] (typename REQUEST_TYPE::pointer request) {
                    server->finish(request->id());
                }
            );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.

        (server->_registry)[request->id()] =
            std::make_shared<RequestWrapperImpl<REQUEST_TYPE>> (request, onFinish);

        // Initiate the request

        request->start ();

        return request;
    }

    /**
     * Return a collection of requests filtered by type.
     */
    template <class REQUEST_TYPE>
    static
    std::vector<typename REQUEST_TYPE::pointer> requestsByType (MasterServer::pointer server) {
    
        std::vector<typename REQUEST_TYPE::pointer> result;
    
        for (auto itr : server->_registry) {
        
            typename REQUEST_TYPE::pointer request =
                std::dynamic_pointer_cast<REQUEST_TYPE>(itr.second->request());
    
            if (request) result.push_back(request);
        }
        return result;
    }
    
    /**
     * Return the number of of requests filtered by type.
     */
    template <class REQUEST_TYPE>
    static
    size_t numRequestsByType (MasterServer::pointer server) {
    
        size_t result{0};
    
        for (auto itr : server->_registry) {
        
            typename REQUEST_TYPE::pointer request =
                std::dynamic_pointer_cast<REQUEST_TYPE>(itr.second->request());

            if (request) ++result;
        }
        return result;
    }
};


////////////////////////////////////////////////////////////////////
//////////////////////////  MasterServer  //////////////////////////
////////////////////////////////////////////////////////////////////


MasterServer::pointer
MasterServer::create (ServiceProvider &serviceProvider) {
    return MasterServer::pointer (
        new MasterServer(serviceProvider));
}

MasterServer::MasterServer (ServiceProvider &serviceProvider)
    :   _serviceProvider (serviceProvider),

        _io_service (),
        _work       (nullptr),
        _thread     (nullptr),
        _registry   () {
}

MasterServer::~MasterServer () {
}

void
MasterServer::run () {

    LOCK_GUARD;

    if (!isRunning()) {

        MasterServer::pointer server = shared_from_this();
     
        _work.reset (
            new boost::asio::io_service::work(_io_service)
        );
        _thread.reset (
            new std::thread (
                [server] () {
        
                    // This will prevent the I/O service from existing the .run()
                    // method event when it will run out of any requess to process.
                    // Unless the service will be explicitly stopped.
    
                    server->_io_service.run();
                    
                    // Always reset the object in a preparation for its further
                    // usage.
    
                    server->_io_service.reset();
                }
            )
        );
    }
}

bool
MasterServer::isRunning () const {
    return _thread.get() != nullptr;
}

void
MasterServer::stop () {

    if (!isRunning()) return;

    // IMPORTANT:
    //
    //   Never attempt running these operations within LOCK_GUARD
    //   due to a possibile deadlock when asynchronous handlers will be
    //   calling the thread-safe methods. A problem is that until they finish
    //   in a clean way (as per the _work.reset()) the thread will never finish,
    //   and the application will hang on _thread->join().

    // LOCK_GUARD  (disabled)

    // Destoring this object will let the I/O service to (eventually) finish
    // all on-going work and shut down the thread. In that case there is no need
    // to stop the service explicitly (which is not a good idea anyway because
    // there may be outstanding synchronous requests, in which case the service
    // would get into an unpredictanle state.)

    _work.reset();

    // Join with the thread before clearning up the pointer

    _thread->join();

    _thread.reset(nullptr);
    
    // Double check that the collection of requests is empty.
    
    if (!_registry.empty())
        throw std::logic_error ("MasterServer::stop() the collection of outstanding requests is not empty");
}

void
MasterServer::join () {
    if (_thread) _thread->join();
}

ReplicationRequest::pointer
MasterServer::replicate (const std::string                 &database,
                         unsigned int                      chunk,
                         const std::string                 &sourceWorkerName,
                         const std::string                 &destinationWorkerName,
                         ReplicationRequest::callback_type  onFinish) {
    LOCK_GUARD;

    assertIsRunning();

    MasterServer::pointer server = shared_from_this();

    ReplicationRequest::pointer request =
        ReplicationRequest::create (
            _serviceProvider,
            database,
            chunk,
            sourceWorkerName,
            destinationWorkerName,
            _io_service,
            [server] (ReplicationRequest::pointer request) {
                server->finish(request->id());
            }
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<ReplicationRequest>> (request, onFinish);

    // Initiate the request

    request->start ();

    return request;
}

DeleteRequest::pointer
MasterServer::deleteReplica (const std::string            &database,
                             unsigned int                  chunk,
                             const std::string            &workerName,
                             DeleteRequest::callback_type  onFinish) {
    LOCK_GUARD;

    assertIsRunning();

    MasterServer::pointer server = shared_from_this();

    DeleteRequest::pointer request =
        DeleteRequest::create (
            _serviceProvider,
            database,
            chunk,
            workerName,
            _io_service,
            [server] (DeleteRequest::pointer request) {
                server->finish(request->id());
            }
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<DeleteRequest>> (request, onFinish);

    // Initiate the request

    request->start ();

    return request;
}

FindRequest::pointer
MasterServer::findReplica (const std::string          &database,
                           unsigned int                chunk,
                           const std::string          &workerName,
                           FindRequest::callback_type  onFinish) {
    LOCK_GUARD;

    assertIsRunning();

    MasterServer::pointer server = shared_from_this();

    FindRequest::pointer request =
        FindRequest::create (
            _serviceProvider,
            database,
            chunk,
            workerName,
            _io_service,
            [server] (FindRequest::pointer request) {
                server->finish(request->id());
            }
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<FindRequest>> (request, onFinish);

    // Initiate the request

    request->start ();

    return request;
}

FindAllRequest::pointer
MasterServer::findAllReplicas (const std::string             &database,
                               const std::string             &workerName,
                               FindAllRequest::callback_type  onFinish) {
    LOCK_GUARD;

    assertIsRunning();

    MasterServer::pointer server = shared_from_this();

    FindAllRequest::pointer request =
        FindAllRequest::create (
            _serviceProvider,
            database,
            workerName,
            _io_service,
            [server] (FindAllRequest::pointer request) {
                server->finish(request->id());
            }
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<FindAllRequest>> (request, onFinish);

    // Initiate the request

    request->start ();

    return request;
}

StopReplicationRequest::pointer
MasterServer::stopReplication (const std::string                     &workerName,
                               const std::string                     &targetRequestId,
                               StopReplicationRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "stopReplication  targetRequestId = " << targetRequestId);

    return MasterServerImpl::requestManagementOperation<StopReplicationRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish);
}

StopDeleteRequest::pointer
MasterServer::stopReplicaDelete (const std::string                &workerName,
                                 const std::string                &targetRequestId,
                                 StopDeleteRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "stopReplicaDelete  targetRequestId = " << targetRequestId);

    return MasterServerImpl::requestManagementOperation<StopDeleteRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish);
}

StopFindRequest::pointer
MasterServer::stopReplicaFind (const std::string              &workerName,
                               const std::string              &targetRequestId,
                               StopFindRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "stopReplicaFind  targetRequestId = " << targetRequestId);

    return MasterServerImpl::requestManagementOperation<StopFindRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish);
}

StopFindAllRequest::pointer
MasterServer::stopReplicaFindAll (const std::string                 &workerName,
                                  const std::string                 &targetRequestId,
                                  StopFindAllRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "stopReplicaFindAll  targetRequestId = " << targetRequestId);

    return MasterServerImpl::requestManagementOperation<StopFindAllRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish);
}

StatusReplicationRequest::pointer
MasterServer::statusOfReplication (const std::string                       &workerName,
                                   const std::string                       &targetRequestId,
                                   StatusReplicationRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfReplication  targetRequestId = " << targetRequestId);

    return MasterServerImpl::requestManagementOperation<StatusReplicationRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish);
}

StatusDeleteRequest::pointer
MasterServer::statusOfDelete (const std::string                  &workerName,
                              const std::string                  &targetRequestId,
                              StatusDeleteRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfDelete  targetRequestId = " << targetRequestId);

    return MasterServerImpl::requestManagementOperation<StatusDeleteRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish);
}

StatusFindRequest::pointer
MasterServer::statusOfFind (const std::string                &workerName,
                            const std::string                &targetRequestId,
                            StatusFindRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfFind  targetRequestId = " << targetRequestId);

    return MasterServerImpl::requestManagementOperation<StatusFindRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish);
}

StatusFindAllRequest::pointer
MasterServer::statusOfFindAll (const std::string                   &workerName,
                               const std::string                   &targetRequestId,
                               StatusFindAllRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfFindAll  targetRequestId = " << targetRequestId);

    return MasterServerImpl::requestManagementOperation<StatusFindAllRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish);
}

ServiceSuspendRequest::pointer
MasterServer::suspendWorkerService (const std::string                    &workerName,
                                    ServiceSuspendRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "suspendWorkerService  workerName: " << workerName);

    return MasterServerImpl::serviceManagementOperation<ServiceSuspendRequest> (
        shared_from_this(),
        workerName,
        onFinish);
}

ServiceResumeRequest::pointer
MasterServer::resumeWorkerService (const std::string                   &workerName,
                                   ServiceResumeRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "resumeWorkerService  workerName: " << workerName);

    return MasterServerImpl::serviceManagementOperation<ServiceResumeRequest> (
        shared_from_this(),
        workerName,
        onFinish);
}

ServiceStatusRequest::pointer
MasterServer::statusOfWorkerService (const std::string                   &workerName,
                                     ServiceStatusRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfWorkerService  workerName: " << workerName);

    return MasterServerImpl::serviceManagementOperation<ServiceStatusRequest> (
        shared_from_this(),
        workerName,
        onFinish);

}

std::vector<ReplicationRequest::pointer>
MasterServer::activeReplicationRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<ReplicationRequest>(shared_from_this());
}

std::vector<DeleteRequest::pointer>
MasterServer::activeDeleteRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<DeleteRequest>(shared_from_this());
}
std::vector<FindRequest::pointer>
MasterServer::activeFindRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<FindRequest>(shared_from_this());
}

std::vector<FindAllRequest::pointer>
MasterServer::activeFindAllRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<FindAllRequest>(shared_from_this());
}

std::vector<StopReplicationRequest::pointer>
MasterServer::activeStopReplicationRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<StopReplicationRequest>(shared_from_this());       
}

std::vector<StopDeleteRequest::pointer>
MasterServer::activeStopDeleteRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<StopDeleteRequest>(shared_from_this());       
}

std::vector<StopFindRequest::pointer>
MasterServer::activeStopFindRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<StopFindRequest>(shared_from_this());       
}

std::vector<StopFindAllRequest::pointer>
MasterServer::activeStopFindAllRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<StopFindAllRequest>(shared_from_this());       
}

std::vector<StatusReplicationRequest::pointer>
MasterServer::activeStatusReplicationRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<StatusReplicationRequest>(shared_from_this());
}

std::vector<StatusDeleteRequest::pointer>
MasterServer::activeStatusDeleteRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<StatusDeleteRequest>(shared_from_this());
}

std::vector<StatusFindRequest::pointer>
MasterServer::activeStatusFindRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<StatusFindRequest>(shared_from_this());
}

std::vector<StatusFindAllRequest::pointer>
MasterServer::activeStatusFindAllRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<StatusFindAllRequest>(shared_from_this());
}

std::vector<ServiceSuspendRequest::pointer>
MasterServer::activeServiceSuspendRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<ServiceSuspendRequest>(shared_from_this());
}


std::vector<ServiceResumeRequest::pointer>
MasterServer::activeServiceResumeRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<ServiceResumeRequest>(shared_from_this());
}


std::vector<ServiceStatusRequest::pointer>
MasterServer::activeServiceStatusRequests () {
    LOCK_GUARD;
    return MasterServerImpl::requestsByType<ServiceStatusRequest>(shared_from_this());
}

size_t
MasterServer::numActiveRequests () {
    LOCK_GUARD;
    return _registry.size();
}


size_t
MasterServer::numActiveReplicationRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<ReplicationRequest>(shared_from_this());
}


size_t
MasterServer::numActiveDeleteRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<DeleteRequest>(shared_from_this());
}

size_t
MasterServer::numActiveFindRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<FindRequest>(shared_from_this());
}

size_t
MasterServer::numActiveFindAllRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<FindAllRequest>(shared_from_this());
}

size_t
MasterServer::numActiveStopReplicationRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<StopReplicationRequest>(shared_from_this());
}

size_t
MasterServer::numActiveStopDeleteRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<StopDeleteRequest>(shared_from_this());
}

size_t
MasterServer::numActiveStopFindRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<StopFindRequest>(shared_from_this());
}

size_t
MasterServer::numActiveStopFindAllRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<StopFindAllRequest>(shared_from_this());
}

size_t
MasterServer::numActiveStatusReplicationRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<StatusReplicationRequest>(shared_from_this());
}

size_t
MasterServer::numActiveStatusDeleteRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<StatusDeleteRequest>(shared_from_this());
}

size_t
MasterServer::numActiveStatusFindRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<StatusFindRequest>(shared_from_this());
}

size_t
MasterServer::numActiveStatusFindAllRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<StatusFindAllRequest>(shared_from_this());
}

size_t
MasterServer::numActiveServiceSuspendRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<ServiceSuspendRequest>(shared_from_this());
}

size_t
MasterServer::numActiveServiceResumeRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<ServiceResumeRequest>(shared_from_this());
}

size_t
MasterServer::numActiveServiceStatusRequests () {
    LOCK_GUARD;
    return MasterServerImpl::numRequestsByType<ServiceStatusRequest>(shared_from_this());
}

void
MasterServer::finish (const std::string &id) {

    // IMPORTANT:
    //
    //   Make sure the notification is complete before removing
    //   the request from the registry. This has two reasons:
    //
    //   - it will avoid a possibility of deadlocking in case if
    //     the callback function to be notified will be doing
    //     any API calls of the server.
    //
    //   - it will reduce the server API dead-time due to a prolonged
    //     execution time of of the callback function.

    RequestWrapper::pointer request;
    {
        LOCK_GUARD;
        request = _registry[id];
        _registry.erase(id);
    }
    request->notify();
}

void
MasterServer::assertIsRunning () const {
    if (!isRunning())
        throw std::runtime_error("the replication service is not running");
}

}}} // namespace lsst::qserv::replica_core
