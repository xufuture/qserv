# Enable private key for the session :
eval `ssh-agent -s`
ssh-add ${HOME}/.ssh/id_rsa

# Create an empty eups packaging shell :
PRODUCT=zookeeper
VERSION=3.4.6

cd dependencies
mkdir p ${PRODUCT}/upstream ${PRODUCT}/ups
cd ${PRODUCT}
git init
git remote rm origin
git remote add origin ssh://git@dev.lsstcorp.org/contrib/eupspkg/${PRODUCT}

# update build procedure, and then :
git add ups upstream
git commit -a -m "eupspkg build procedure for ${PRODUCT}"
git pom
git tag -f ${VERSION} 
git push -f --tags
git tag | head -1 | xargs git tag -f; git push --tags
