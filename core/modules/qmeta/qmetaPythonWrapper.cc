// Class header

// System headers

// Third-party headers
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

// LSST headers

// Qserv headers
#include "qmeta/Exceptions.h"
#include "qmeta/QInfo.h"
#include "qmeta/QMeta.h"

namespace py = pybind11;
using namespace pybind11::literals;

namespace lsst {
namespace qserv {
namespace qmeta {

PYBIND11_PLUGIN(qmetaLib) {

    py::module mod("qmetaLib");

    py::class_<QInfo> clsQInfo(mod, "QInfo");

    // enums need to be defined first because constructor has a default
    // parameter of enum type
    py::enum_<QInfo::QType>(clsQInfo, "QType")
        .value("SYNC", QInfo::QType::SYNC)
        .value("ASYNC", QInfo::QType::ASYNC)
        .value("ANY", QInfo::QType::ANY)
        .export_values()
        ;

    py::enum_<QInfo::QStatus>(clsQInfo, "QStatus")
        .value("EXECUTING", QInfo::QStatus::EXECUTING)
        .value("COMPLETED", QInfo::QStatus::COMPLETED)
        .value("FAILED", QInfo::QStatus::FAILED)
        .value("ABORTED", QInfo::QStatus::ABORTED)
        .export_values()
        ;

    clsQInfo
        .def(py::init<>())
        .def(py::init<QInfo::QType, CzarId, std::string const&,
                std::string const&, std::string const&,
                std::string const&, std::string const&,
                std::string const&, std::string const&,
                QInfo::QStatus, std::time_t, std::time_t, std::time_t>(),
                "qType"_a, "czarId"_a, "user"_a, "qText"_a, "qTemplate"_a,
                "qMerge"_a, "qProxyOrderBy"_a, "resultLoc"_a, "msgTableName"_a,
                "qStatus"_a=QInfo::EXECUTING, "submitted"_a=std::time_t(0),
                "completed"_a=std::time_t(0), "returned"_a=std::time_t())
        .def("queryType", &QInfo::queryType)
        .def("queryStatus", &QInfo::queryStatus)
        .def("czarId", &QInfo::czarId)
        .def("user", &QInfo::user)
        .def("queryText", &QInfo::queryText)
        .def("queryTemplate", &QInfo::queryTemplate)
        .def("mergeQuery", &QInfo::mergeQuery)
        .def("proxyOrderBy", &QInfo::proxyOrderBy)
        .def("resultLocation", &QInfo::resultLocation)
        .def("msgTableName", &QInfo::msgTableName)
        .def("submitted", &QInfo::submitted)
        .def("completed", &QInfo::completed)
        .def("returned", &QInfo::returned)
        .def("duration", &QInfo::duration)
        ;

    py::class_<QMeta, std::shared_ptr<QMeta>>(mod, "QMeta")
        .def_static("createFromConfig", &QMeta::createFromConfig)
        .def("getCzarID", &QMeta::getCzarID)
        .def("registerCzar", &QMeta::registerCzar)
        .def("setCzarActive", &QMeta::setCzarActive)
        .def("registerQuery", &QMeta::registerQuery)
        .def("addChunks", &QMeta::addChunks)
        .def("assignChunk", &QMeta::assignChunk)
        .def("finishChunk", &QMeta::finishChunk)
        .def("completeQuery", &QMeta::completeQuery)
        .def("finishQuery", &QMeta::finishQuery)
        .def("findQueries", &QMeta::findQueries,
                "czarId"_a=0, "qType"_a=QInfo::ANY, "user"_a=std::string(),
                "status"_a=std::vector<QInfo::QStatus>(),
                "completed"_a=-1, "returned"_a=-1)
        .def("getPendingQueries", &QMeta::getPendingQueries)
        .def("getQueryInfo", &QMeta::getQueryInfo)
        .def("getQueriesForDb", &QMeta::getQueriesForDb)
        .def("getQueriesForTable", &QMeta::getQueriesForTable)
        ;

    // Exception classes
    static py::exception<Exception> Exception(mod, "Exception");
    py::register_exception<CzarNameError>(mod, "CzarNameError", Exception.ptr());
    py::register_exception<CzarIdError>(mod, "CzarIdError", Exception.ptr());
    py::register_exception<QueryIdError>(mod, "QueryIdError", Exception.ptr());
    py::register_exception<ChunkIdError>(mod, "ChunkIdError", Exception.ptr());
    py::register_exception<SqlError>(mod, "SqlError", Exception.ptr());
    py::register_exception<MissingTableError>(mod, "MissingTableError", Exception.ptr());
    py::register_exception<ConsistencyError>(mod, "ConsistencyError", Exception.ptr());

    return mod.ptr();
}

}}} // namespace lsst::qserv::qmeta
