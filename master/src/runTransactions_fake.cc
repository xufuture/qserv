// Fake, no-op versions to allow certain builds to link.
#include "lsst/qserv/master/QuerySession.h"
using lsst::qserv::master::QuerySession;

QuerySession::QuerySession() {}

void QuerySession::setQuery(std::string const& q) {}
bool QuerySession::getHasAggregate() const { return false; }
lsst::qserv::master::ConstraintVector QuerySession::getConstraints() const {
    return ConstraintVector(); }
void QuerySession::addChunk(ChunkSpec const& cs) {}

