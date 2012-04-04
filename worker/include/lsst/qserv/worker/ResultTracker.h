/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
#ifndef LSST_QSERV_WORKER_RESULT_TRACKER_H
#define LSST_QSERV_WORKER_RESULT_TRACKER_H
#include <deque>
#include <iostream>

#include <boost/signal.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include "lsst/qserv/worker/WorkQueue.h"

namespace lsst {
namespace qserv {
namespace worker {

struct ResultError {
    ResultError() {}
    ResultError(int chunkId_, int code_, std::string const& desc_) 
    : chunkId(chunkId_), code(code_), desc(desc_) {}
    
    int chunkId;
    int code;
    std::string desc;
};
typedef std::pair<int,char const*> ResultItem;
typedef boost::shared_ptr<ResultError> ResultErrorPtr;

// Make sure Item is a primitive that can be copied.
template <typename Key, typename Item>
class ResultTracker {
public:
    ////////////////////////////////////////////////////
    // Typedefs and inner classes
    typedef boost::signal<void (Item)> Signal;
    struct LockableSignal {
    public:
        typedef std::deque<boost::signals::connection> CDeque;
        boost::mutex mutex;
        Signal signal;
        CDeque connections; // Remember connections so we can disconnect.
        void clearListeners() {
            CDeque::iterator i, end;
            for(i=connections.begin(), end=connections.end();
                i != end; ++i) {
                i->disconnect();
            }
        }
    };
    // Wrap up a notification into a no-argument functor that can be queued.
    template <class C>
    class ResultCallable : public WorkQueue::Callable {
    public:
        typedef boost::shared_ptr<ResultCallable> Ptr;
        ResultCallable(C const& c, Item const& i) : _c(c), _i(i) {}
        virtual void operator()() { _c(_i); }
        C _c;
        Item _i;
    };
    ////////////////////////////////////////////////////
    typedef boost::shared_ptr<Item> ItemPtr;
    typedef boost::shared_ptr<LockableSignal> LSPtr;
    typedef std::map<Key, LSPtr> SignalMap;
    typedef std::map<Key, Item> NewsMap;
    //////////////////////////////////////////////////
    // Methods
    //////////////////////////////////////////////////
    // Constructor
    ResultTracker() : _workQueue(3) {} // Callback pool w/ 3 threads

    // Notify everyone that an item i for key k is ready
    void notify(Key const& k, Item const& i) {
        _verifyKey(k); // Force k to exist in _signals
        LSPtr s;
        {
            boost::unique_lock<boost::mutex> lock(_signalsMutex);
            s = _signals[k];
        }
        {
            boost::unique_lock<boost::mutex> slock(s->mutex);
            //std::cerr << "Callback (tracker) signalling " 
            //	      << k << " ---- " << std::endl;
            s->signal(i); // Notify listeners
            s->clearListeners();
            // Save news item
            boost::unique_lock<boost::mutex> nlock(_newsMutex);
            _news[k] = i;
        }
    }

    // Destroy news items for key k
    void clearNews(Key const& k) {
        boost::unique_lock<boost::mutex> lock(_newsMutex);
        typename NewsMap::iterator i = _news.find(k);
        if(i != _news.end()) {
            _news.erase(i);
        }
    }

    // Register a callback to be fired when key k has news
    template <typename Callable>
    void listenOnce(Key const& k, Callable const& c) {
        {
            boost::unique_lock<boost::mutex> lock(_newsMutex);
            typename NewsMap::iterator i = _news.find(k);
            if(i != _news.end()) { // If already reported, reuse.
                boost::shared_ptr<ResultCallable<Callable> > rc;
                rc.reset(new ResultCallable<Callable>(c,i->second));
                _workQueue.add(rc);
                return;
            }
        }
        _verifyKey(k);
        LSPtr s;
        {
            boost::unique_lock<boost::mutex> slock(_signalsMutex);
            s = _signals[k];
        }
        {
            boost::unique_lock<boost::mutex> lock(s->mutex);
            // Check again, in case there was a notification.
            boost::unique_lock<boost::mutex> nlock(_newsMutex);
            typename NewsMap::iterator i = _news.find(k);
            if(i != _news.end()) { 
                boost::shared_ptr<ResultCallable<Callable> > rc;
                rc.reset(new ResultCallable<Callable>(c,i->second));
                _workQueue.add(rc);
                return;
            } else {
                // No news, so subscribe.
                s->connections.push_back(s->signal.connect(c)); 
            }
        }
    }

    // Check to see whether key k has news
    ItemPtr getNews(Key const& k) {
        ItemPtr p;
        boost::unique_lock<boost::mutex> lock(_newsMutex);
        typename NewsMap::iterator i = _news.find(k);
        if(i != _news.end()) { 
            p = boost::make_shared<Item>(i->second);
        }
        return p;
    }

    int getNewsCount() {
        boost::unique_lock<boost::mutex> lock(_newsMutex);
        return _news.size(); // 
    }
    int getSignalCount() {
        boost::unique_lock<boost::mutex> lock(_signalsMutex);
        return _signals.size();
    }
    // Debug methods
    NewsMap& debugGetNews() { return _news; }
    SignalMap& debugGetSignals() { return _signals; }
    void debugReset() {
        _signals.clear();
        _news.clear();
    }
private:
    void _verifyKey(Key const& k) {
        boost::unique_lock<boost::mutex> lock(_signalsMutex);
        if(_signals.find(k) == _signals.end()) {
            _signals[k] = boost::make_shared<LockableSignal>();
        }
    }

    SignalMap _signals;
    NewsMap _news;
    boost::mutex _signalsMutex;
    boost::mutex _newsMutex;
    WorkQueue _workQueue;
};
    
}}} // namespace lsst::qserv::worker

#endif // LSST_QSERV_WORKER_RESULT_TRACKER_H
