# Enable private key for the session :
eval `ssh-agent -s`
ssh-add ${HOME}/.ssh/id_rsa

# Create an empty eups packaging shell :
PRODUCT=kazoo
VERSION=1.3.1

cd dependencies &&
mkdir -p ${PRODUCT}/upstream ${PRODUCT}/ups &&
cd ${PRODUCT} &&
git init &&
git remote add origin ssh://git@dev.lsstcorp.org/contrib/eupspkg/${PRODUCT} 

# update build procedure, and then :
git add ups upstream
git commit -a -m "eupspkg build procedure for ${PRODUCT}"
git tag -f ${VERSION} 
git push origin master -f --tags
# or
git tag | head -1 | xargs git tag -f; git push origin master -f --tags
