eups_install() {

    echo  "Installing eups using : '$EUPS_GIT_CLONE_CMD'"

    # cleaning
    if [ -e "${INSTALL_DIR}/eups/bin/setups.sh" ]
    then   
        echo "Removing previous install"
        source "${INSTALL_DIR}/eups/bin/setups.sh"
        eups_unsetup_all
        eups_remove_all
    fi

    rm -rf ~/.eups/ups_db ~/.eups/_caches_

    # installing eups latest version
    cd ${INSTALL_DIR}
    rm -rf sources &&
    mkdir sources &&
    cd sources &&
    ${EUPS_GIT_CLONE_CMD} &&
    cd eups/ &&
    ${EUPS_GIT_CHECKOUT_CMD} &&
    ./configure --prefix="${INSTALL_DIR}/eups" \
    --with-eups="${INSTALL_DIR}/stack"&&
    make &&
    make install ||
    {
        echo "Failed to install eups" >&2
        exit 1
    }
}

eups_dist() {
    if [ -z "$1" -o -z "$2" ]; then
        echo "eups_dist requires two arguments"
        exit 1
    fi
    
    product=$1 &&
    version=$2 &&
    CWD=${PWD} &&
    TMP_DIR=${INSTALL_DIR}/tmp &&
    mkdir -p ${TMP_DIR} &&
    cd ${TMP_DIR} &&
    rm -rf ${product} &&
    git clone ${REPOSITORY_BASE}/${product} &&
    cd ${product} &&
    git checkout -q ${version} -- ups &&
    cmd="eups declare ${product} ${version} -r ." &&
    echo "CMD : $cmd" &&
    $cmd &&
    cmd="eups distrib create --nodepend --server-dir=${LOCAL_PKGROOT} -f generic -d eupspkg -t current ${product}"
    echo "Running : $cmd" &&
    $cmd &&
    # for debug purpose only : build file generation
    # eups expandbuild -V ${version} ups/${product}.build > ${product}-${version}.build
    cd ${CWD}
}

eups_remove_all() {
    echo "INFO : removing all packages except git"
    eups list  | grep -v git | cut -f1 |  awk '{print "eups remove -t current --force "$1}' | bash
}

eups_unsetup_all() {
    echo "INFO : unsetup of all packages"
    eups list | grep -w setup | cut -f1 |  awk '{print "unsetup "$1}' | bash
}

upload_to_distserver() {
    cp ${QSERV_SRC_DIR}/eupspkg/dist/.htaccess ${LOCAL_PKGROOT}
    lftp -e "mirror -Re ${LOCAL_PKGROOT} www/htdocs/qserv/; quit" sftp://datasky:od39yW0e@datasky.in2p3.fr/
}

check_java_version() {

    if [ -z "$1" ]; then
        echo "check_java_version() require one argument" >&2
        return 1
    fi

    min_java_version=$1
    JAVA_OK=1
    if type -p java; then
        echo "Found java executable in PATH"
        _java=java
    elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
        echo Found java executable in JAVA_HOME     
        _java="$JAVA_HOME/bin/java"
    else
        echo "System java not found"
    fi

    if [[ "$_java" ]]; then
        version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
        echo version "$version"
        if [[ "$version" == ${min_java_version}* ]] || [[ "$version" > "${min_java_version}" ]]; then
            echo "Java version is more than ${min_java_version}"
            JAVA_OK=0        
        else         
            echo "Java version is less than ${min_java_version}"
        fi
    fi
    return $JAVA_OK
}
