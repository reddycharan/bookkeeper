#!/bin/bash
################################################################################
#
# sfstore_version.sh
#
# Script that drives the sfstore version in Jenkins
#
# Copyright (c) 2016, Salesforce.com
#
# Author: Rithin Shetty
#
################################################################################

# This script reads the current sfstore major,minor,patch versions from the
# sfstore git repository, renames the sfstore and proxy jar files and the deploy
# tar balls so that they contain the latest version number. It then bumps the
# minor version by 1 abd commits the new minor version number to the sfstore git
# repository. So that the next we create a release build, it will get the next
# minor version number. It also adds a tag corresponding to the current image.

ARTIFACT_DIR=$WORKSPACE

[ -z "$COPY_FROM_JOB" ]   && echo "Error! \$COPY_FROM_JOB not set. Which job you want to copy artifact from?"  && exit -1
[ -z "$DEST_DIR" ]        && echo "Error! \$DEST_DIR not set."  && exit -1
[ -z "$COPY_FROM_BUILD" ] && echo "Error! \$BUILD_NUMBER_TO_COPY_FROM not set."  && exit -1
[[ -z "$IMAGE_NAME1" ]]      && echo "Error! \$IMAGE_NAME1 not set."  && exit -1
[ -z "$TARGET_BRANCH" ] && echo "Error! \$TARGET_BRANCH not set." && exit -1
[ -z "$UPDATE_GIT_TAGS" ] && echo "Error!  $UPDATE_GIT_TAGS was not set." && exit -1
[ -z "$SFSTORE_ERELEASE" ] && echo "Error!  \$SFSTORE_ERELEASE was not set." && exit -1

if [[ ! -f $ARTIFACT_DIR/$IMAGE_NAME1 ]];then
   echo "!!!ERROR: Artifacts were not copied from Build job. Please check"
   exit 1;
fi

# Check if the target_branch does exist
echo "Checking if branch ${TARGET_BRANCH} exists..."
git rev-parse --verify ${TARGET_BRANCH}
if [ $? != 0 ];then
  echo "ERROR! ${TARGET_BRANCH} is not present in git remote."
  exit -1
fi

WORKSPACEIMAGEDIR=$WORKSPACE/tmpinstall
mkdir -p $WORKSPACEIMAGEDIR
# delete any files in case this directory already existed
rm -rf $WORKSPACEIMAGEDIR/*

DEPLOY_SFSTORE=${WORKSPACEIMAGEDIR}/deploy-sfstore
mkdir -p ${DEPLOY_SFSTORE}
rm -rf ${DEPLOY_SFSTORE}/*

cp ${ARTIFACT_DIR}/${IMAGE_NAME1} ${DEPLOY_SFSTORE}

cd $DEPLOY_SFSTORE
tar -xf $IMAGE_NAME1
rm ${IMAGE_NAME1}

# SFStore-Build jenkin job creates file : gitlog.info, containing the last git commit SHA, and tars it to $IMAGE_NAME1
GIT_COMMIT=`cat gitlog.info`
[ -z "$GIT_COMMIT" ] && echo "Error! \$GIT_COMMIT not set, which means gitlog.info file is not created properly by SFStore-Build jenkin job."  && exit -1
echo "This new SFStore-Gold-Image is based on git commit SHA : $GIT_COMMIT"

echo "sfstore.version.info file content:"
SFSTORE_VERSION_INFO=${WORKSPACE}/sfstore.version.info
cat ${SFSTORE_VERSION_INFO}
SFSTORE_MAJOR_VERSION=`grep "^SFSTORE_MAJOR_VERSION=" ${SFSTORE_VERSION_INFO} | awk -F"=" '{ print $2 }'`
echo "major=${SFSTORE_MAJOR_VERSION}"
SFSTORE_MINOR_VERSION=`grep "^SFSTORE_MINOR_VERSION=" ${SFSTORE_VERSION_INFO} | awk -F"=" '{ print $2 }'`
echo "minor=${SFSTORE_MINOR_VERSION}"
SFSTORE_PATCH_VERSION=`grep "^SFSTORE_PATCH_VERSION=" ${SFSTORE_VERSION_INFO} | awk -F"=" '{ print $2 }'`
echo "patch=${SFSTORE_PATCH_VERSION}"
CURR_VERSION_STRING=${SFSTORE_MAJOR_VERSION}.${SFSTORE_MINOR_VERSION}.${SFSTORE_PATCH_VERSION}

# If this is not an eRelease, create a new branch for the release
if [ "x$SFSTORE_ERELEASE" != "xtrue" ];then
  RELEASE_BRANCH="SFSTORE_${SFSTORE_MAJOR_VERSION}.${SFSTORE_MINOR_VERSION}"
  echo "RELEASE_BRANCH=${RELEASE_BRANCH}"

  echo "Testing to see if branch ${RELEASE_BRANCH} already exists..."
  if [ ! -z $(git rev-parse --verify ${RELEASE_BRANCH} 2> /dev/null) ]; then
    echo "ERROR! Branch ${RELEASE_BRANCH} already exists!"
    exit -1
  fi

  echo "Creating new branch..."
  git checkout -b ${RELEASE_BRANCH} ${GIT_COMMIT}
  if [ ! "$(git rev-parse --abbrev-ref HEAD)" == "${RELEASE_BRANCH}" ];then
    echo "ERROR! Couldn't create local branch ${RELEASE_BRANCH}."
    exit -1
  fi

  git push -u origin ${RELEASE_BRANCH}

  BRANCH_EXISTS=$(git ls-remote --heads origin ${RELEASE_BRANCH})
  if [ -z "${BRANCH_EXISTS}" ];then
    echo -e "ERROR! Branch ${RELEASE_BRANCH} not created successfully."
    exit -1
  else
    echo -e "\n\n Branch ${RELEASE_BRANCH} Created and pushed to remote successfully \n\n"
  fi
fi

# Switch to initial target_branch
git checkout ${TARGET_BRANCH}

# rename the jar files and repackage it with the right version string
cd $DEPLOY_SFSTORE
mv bookkeeper-server*.jar bookkeeper-server-${CURR_VERSION_STRING}.jar
NEW_SFSTORE_DEPLOY_NAME=deploy-sfstore-${CURR_VERSION_STRING}.tar
tar cf ${ARTIFACT_DIR}/${NEW_SFSTORE_DEPLOY_NAME} *
bzip2 -zfq ${ARTIFACT_DIR}/${NEW_SFSTORE_DEPLOY_NAME}

# Update git tags
if [ "x$UPDATE_GIT_TAGS" = "xtrue" ];then
  cd $WORKSPACE
  echo "Tagging the version number for sfstore"
  GIT_GOLD_IMAGE_TAG=Gold_Image_${CURR_VERSION_STRING}
  git tag $GIT_GOLD_IMAGE_TAG $GIT_COMMIT

  SFSTORE_PATCH_VERSION_PLUS_ONE=$((SFSTORE_PATCH_VERSION+1))
  SFSTORE_MINOR_VERSION_PLUS_ONE=$((SFSTORE_MINOR_VERSION+1))
  # Check if eRelease in which case update the patch version while retaining the previous minor version
  if [ "x$SFSTORE_ERELEASE" = "xtrue" ];then
  NEW_VERSION_STRING=${SFSTORE_MAJOR_VERSION}.${SFSTORE_MINOR_VERSION}.${SFSTORE_PATCH_VERSION_PLUS_ONE}
  else
  NEW_VERSION_STRING=${SFSTORE_MAJOR_VERSION}.${SFSTORE_MINOR_VERSION_PLUS_ONE}.${SFSTORE_PATCH_VERSION}
  fi

  if [ "x$SFSTORE_ERELEASE" = "xtrue" ];then
    # Update only the target branch in case of eRelease

    # Checkout the target branch(in this case SFSTORE_(MAJOR).(MINOR) ). Update the patch version in the release branch. Commit and push the change to the repo. Show the new sfstore.version.info
    git checkout ${TARGET_BRANCH}
    sed -i -E "s/SFSTORE_PATCH_VERSION=$SFSTORE_PATCH_VERSION/SFSTORE_PATCH_VERSION=$SFSTORE_PATCH_VERSION_PLUS_ONE/" sfstore.version.info
    git add sfstore.version.info
    git commit -m "(@bug W-3302860@) Tag $GIT_GOLD_IMAGE_TAG and update SFSTORE_PATCH_VERSION to $SFSTORE_PATCH_VERSION_PLUS_ONE"
    if $(git push origin $TARGET_BRANCH)
    then
    echo "Successfully pushed the updated version number to the target branch"
    else
    echo "Push to the target branch failed. Please check"
     fi
  else
    # Update the target branch(sfstore) as well as the release branch created in case of normal release

    # Checkout the target branch(in this case sfstore). Update the minor version in the sfstore branch. Commit and push the change to the repo. Show the new sfstore.version.info
    git checkout ${TARGET_BRANCH}
    sed -i -E "s/SFSTORE_MINOR_VERSION=$SFSTORE_MINOR_VERSION/SFSTORE_MINOR_VERSION=$SFSTORE_MINOR_VERSION_PLUS_ONE/" sfstore.version.info
    git add sfstore.version.info
    git commit -m "(@bug W-3302860@) Tag $GIT_GOLD_IMAGE_TAG and update SFSTORE_MINOR_VERSION to $SFSTORE_MINOR_VERSION_PLUS_ONE"
    if $(git push origin $TARGET_BRANCH)
    then
    echo "Successfully pushed the updated version number to the target branch"
    else
    echo "Push to the target branch failed. Please check"
     fi
    echo "New version of sfstore.version.info checked in to repo in branch ${TARGET_BRANCH} is:"
    cat sfstore.version.info

    # Checkout the target branch(in this case SFSTORE_(MAJOR).(MINOR) ). Update the patch version in the release branch. Commit and push the change to the repo. Show the new sfstore.version.info
    git checkout ${RELEASE_BRANCH}
    sed -i -E "s/SFSTORE_PATCH_VERSION=$SFSTORE_PATCH_VERSION/SFSTORE_PATCH_VERSION=$SFSTORE_PATCH_VERSION_PLUS_ONE/" sfstore.version.info
    git add sfstore.version.info
    git commit -m "(@bug W-3302860@) Tag $GIT_GOLD_IMAGE_TAG and update SFSTORE_PATCH_VERSION to $SFSTORE_PATCH_VERSION_PLUS_ONE"
    if $(git push origin $RELEASE_BRANCH)
    then
    echo "Successfully pushed the updated version number to the release branch"
    else
    echo "Push to the release branch failed. Please check"
     fi
    echo "New version of sfstore.version.info checked in to repo in branch ${RELEASE_BRANCH} is:"
    cat sfstore.version.info
  fi

  git push --tags
else
  echo "You chose not to update git tags.."
fi

# delete the old deploy packages that didn't have the version number
rm ${ARTIFACT_DIR}/${IMAGE_NAME1}
