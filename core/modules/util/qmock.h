// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
/**
 * @file
 *
 * @ingroup util
 *
 * @brief Macros to facilitate mocking for tests
 *
 * @author Fritz Mueller, SLAC
 */

#ifndef LSST_QSERV_UTIL_QMOCK_H
#define LSST_QSERV_UTIL_QMOCK_H

#define QMOCK_VARIADIC_0(e0, ...) e0
#define QMOCK_VARIADIC_1(e0, e1, ...) e1

//-----------------------------------------------------------------------------

#define QMOCK_STUB_0(REAL_CLASS, METHOD_NAME, METHOD_SIG, CONST)              \
    boost::function<METHOD_SIG>::result_type REAL_CLASS::METHOD_NAME() CONST  \
    {                                                                         \
        return REAL_CLASS##Mock::GetMockFor(this).METHOD_NAME();              \
    }                                                                         \

#define QMOCK_STUB_1(REAL_CLASS, METHOD_NAME, METHOD_SIG, CONST)              \
    boost::function<METHOD_SIG>::result_type REAL_CLASS::METHOD_NAME(         \
        boost::function<METHOD_SIG>::arg1_type a1) CONST                      \
    {                                                                         \
        return REAL_CLASS##Mock::GetMockFor(this).METHOD_NAME(a1);            \
    }                                                                         \

#define QMOCK_STUB_2(REAL_CLASS, METHOD_NAME, METHOD_SIG, CONST)              \
    boost::function<METHOD_SIG>::result_type REAL_CLASS::METHOD_NAME(         \
        boost::function<METHOD_SIG>::arg1_type a1,                            \
        boost::function<METHOD_SIG>::arg2_type a2) CONST                      \
    {                                                                         \
        return REAL_CLASS##Mock::GetMockFor(this).METHOD_NAME(a1, a2);        \
    }                                                                         \

#define QMOCK_STUB_3(REAL_CLASS, METHOD_NAME, METHOD_SIG, CONST)              \
    boost::function<METHOD_SIG>::result_type REAL_CLASS::METHOD_NAME(         \
        boost::function<METHOD_SIG>::arg1_type a1,                            \
        boost::function<METHOD_SIG>::arg2_type a2,                            \
        boost::function<METHOD_SIG>::arg3_type a3) CONST                      \
    {                                                                         \
        return REAL_CLASS##Mock::GetMockFor(this).METHOD_NAME(a1, a2, a3);    \
    }                                                                         \

#define EXPAND_QMOCK_STUB(REAL_CLASS, METHOD, ARITY, ...)                     \
    QMOCK_STUB_##ARITY(                                                       \
        REAL_CLASS,                                                           \
        METHOD,                                                               \
        QMOCK_VARIADIC_0(__VA_ARGS__,),                                       \
        QMOCK_VARIADIC_1(__VA_ARGS__,,))                                      \

#define MOCK_DEFINE_STUBS(REAL_CLASS, MOCK_METHODS)                           \
    MOCK_METHODS(EXPAND_QMOCK_STUB)                                           \

//-----------------------------------------------------------------------------

#define QMOCK_CLASS_0(REAL_CLASS, METHOD_NAME, METHOD_SIG, CONST)             \
    virtual boost::function<METHOD_SIG>::result_type METHOD_NAME() CONST      \
    {                                                                         \
        return boost::function<METHOD_SIG>::result_type();                    \
    }                                                                         \

#define QMOCK_CLASS_1(REAL_CLASS, METHOD_NAME, METHOD_SIG, CONST)             \
    virtual boost::function<METHOD_SIG>::result_type METHOD_NAME(             \
        boost::function<METHOD_SIG>::arg1_type) CONST                         \
    {                                                                         \
        return boost::function<METHOD_SIG>::result_type();                    \
    }                                                                         \

#define QMOCK_CLASS_2(REAL_CLASS, METHOD_NAME, METHOD_SIG, CONST)             \
    virtual boost::function<METHOD_SIG>::result_type METHOD_NAME(             \
        boost::function<METHOD_SIG>::arg1_type,                               \
        boost::function<METHOD_SIG>::arg2_type) CONST                         \
    {                                                                         \
        return boost::function<METHOD_SIG>::result_type();                    \
    }                                                                         \

#define QMOCK_CLASS_3(REAL_CLASS, METHOD_NAME, METHOD_SIG, CONST)             \
    virtual boost::function<METHOD_SIG>::result_type METHOD_NAME(             \
        boost::function<METHOD_SIG>::arg1_type,                               \
        boost::function<METHOD_SIG>::arg2_type,                               \
        boost::function<METHOD_SIG>::arg3_type) CONST                         \
    {                                                                         \
        return boost::function<METHOD_SIG>::result_type();                    \
    }                                                                         \

#define EXPAND_QMOCK_CLASS(REAL_CLASS, METHOD, ARITY, ...)                    \
    QMOCK_CLASS_##ARITY(                                                      \
        REAL_CLASS,                                                           \
        METHOD,                                                               \
        QMOCK_VARIADIC_0(__VA_ARGS__,),                                       \
        QMOCK_VARIADIC_1(__VA_ARGS__,,))                                      \

#define MOCK_DEFINE_MOCK(REAL_CLASS, MOCK_METHODS)                                           \
    class REAL_CLASS##Mock                                                                   \
    {                                                                                        \
    public:                                                                                  \
        static REAL_CLASS##Mock &GetMockFor(const REAL_CLASS* real) {                        \
            static std::map<const REAL_CLASS*, REAL_CLASS##Mock*> mockMap;                   \
            std::map<const REAL_CLASS*, REAL_CLASS##Mock*>::iterator i = mockMap.find(real); \
            if (i != mockMap.end()) return *(i->second);                                     \
            REAL_CLASS##Mock *mock = new REAL_CLASS##Mock();                                 \
            mockMap[real] = mock;                                                            \
            return *mock;                                                                    \
        }                                                                                    \
        MOCK_METHODS(EXPAND_QMOCK_CLASS)                                                     \
    };                                                                                       \

#endif // LSST_QSERV_UTIL_QMOCK_H
