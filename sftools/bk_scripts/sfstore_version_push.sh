#!/bin/bash -e

ARTIFACT_DIR=$WORKSPACE

[ -z "$GUSURL" ]          && echo "Error! \$GUSURL not set."  && exit -1
[ -z "$COPY_FROM_JOB" ]   && echo "Error! \$COPY_FROM_JOB not set. Which job you want to copy artifact from?"  && exit -1
[[ -z "$SFSTORE_DEST_DIR" || -z "$PROXY_DEST_DIR" ]]        && echo "Error! \$DEST_DIR not set."  && exit -1
[ -z "$COPY_FROM_BUILD" ] && echo "Error! \$BUILD_NUMBER_TO_COPY_FROM not set."  && exit -1
[[ -z "$SFSTORE_IMAGE" && -z "$PROXY_IMAGE" ]]      && echo "Error! \$SFSTORE_IMAGE and \$PROXY_IMAGE not set."  && exit -1
[ -z "$P4CLIENT" ]        && echo "Error!  \$P4CLIENT not set." && exit -1
[ -z "$P4USER" ]          && echo "Error!  \$P4USER not set." && exit -1


echo "P4CLIENT: $P4CLIENT"
echo "P4USER: $P4USER"

# SFM does not use blt but Ubuntu uses it.
if [ -d $HOME/blt ]; then
    echo "Using BLT directory"
    PROXY_INSTALL_DIR=$HOME/blt/$PROXY_DEST_DIR
    SFSTORE_INSTALL_DIR=$HOME/blt/$SFSTORE_DEST_DIR
    INSTALL_BASE_DIR=$HOME/blt
else
    PROXY_INSTALL_DIR=$HOME/$PROXY_DEST_DIR
    SFSTORE_INSTALL_DIR=$HOME/$SFSTORE_DEST_DIR
    INSTALL_BASE_DIR=$HOME
fi

echo "Proxy install directory: $PROXY_DEST_DIR"
echo "SFStore install directory: $SFSTORE_DEST_DIR"

SFSTORE_IMAGE_PRESENT=`ls -1 | grep -i $SFSTORE_IMAGE | wc -l`
PROXY_IMAGE_PRESENT=`ls -1 | grep -i $PROXY_IMAGE | wc -l`
if [[ SFSTORE_IMAGE_PRESENT -ne 1 && PROXY_IMAGE_PRESENT -ne 1 ]]; then
  echo "Either $SFSTORE_IMAGE or $PROXY_IMAGE not copied or has duplicates in directory. Must be only one present. Exiting."
  exit -1;
else
  #We use regexes in the package search. Use those to get the whole name. 
  export SFSTORE_IMAGE=`ls -1 | grep $SFSTORE_IMAGE`
  export PROXY_IMAGE=`ls -1 | grep $PROXY_IMAGE`
  echo "Artifacts present. SFstore image: $SFSTORE_IMAGE Proxy image: $PROXY_IMAGE. Proceeding."
fi

CURRENT_VERSION=`ls -1 | grep $SFSTORE_IMAGE | grep -o '[0-9]\{1,3\}[.]\{1\}[0-9]\{1,3\}[.]\{1\}[0-9]\{1,3\}'`
echo "Current SFStore version: $CURRENT_VERSION"
ls -l $ARTIFACT_DIR
    
# Sync before we attempt to edit
echo -e "\n\n Force syncing images from perforce"
p4 sync -f //$PROXY_DEST_DIR/$PROXY_IMAGE
if [ "$?" -ne 0 ];then
   echo "Failed to do P4 for $PROXY_IMAGE. You may have to check p4 setup" && exit -1;
fi

p4 sync -f //$SFSTORE_DEST_DIR/$SFSTORE_IMAGE
if [ "$?" -ne 0 ];then
   echo "Failed to do P4 sync for $SFSTORE_IMAGE. You may have to check p4 setup" && exit -1;
fi

p4 sync -f //$PROXY_PROP_DIR/$PROXY_PROP_FILE
if [ "$?" -ne 0 ];then
   echo "Failed to do P4 sync for $PROXY_PROP_FILE. You may have to check p4 setup" && exit -1;
fi

p4 sync -f //$SFSTORE_PROP_DIR/$SFSTORE_PROP_FILE
if [ "$?" -ne 0 ];then
   echo "Failed to do P4 sync for $SFSTORE_PROP_FILE. You may have to check p4 setup" && exit -1;
fi

echo "Synced all files"

#If either one of them isn't present, that means we need to create a changelist file because 
#at least one will be checked in.
echo "Checking for $PROXY_INSTALL_DIR/$PROXY_IMAGE and $SFSTORE_INSTALL_DIR/$SFSTORE_IMAGE"

echo "Creating p4 check-in file."
  # Create a temp file that will be used to set up the changelist
p4infile=$WORKSPACE/p4change_$$.txt
if [ -e $p4infile ]; then
  rm $p4infile
fi

if [ -e $p4infile ]; then
  echo "Could not delete $p4infile" && exit -1
fi

  # Create an input file that is used for the changelist specification -- left-aligned. 
  echo "
Change: new
Client: $P4CLIENT
User:   $P4USER
Status: new

Description:
  $COPY_FROM_JOB
  $PROXY_IMAGE
  @precheckin bypass@
  @rev none@
  $GUSURL
  Job: Publish-SFStore-Gold-Image
  Build number: ${CURRENT_VERSION}
    " > $p4infile


if [ -d $PROXY_INSTALL_DIR ];then
   cd $PROXY_INSTALL_DIR
else
   echo "ERROR: $PROXY_INSTALL_DIR not found. Something wrong."
   exit -1;
fi


# Create the changelist
changelist=`cat $p4infile | p4 change -i | awk '{print $2}'`
echo "Changelist>"$changelist"<"

#If it already exists, mark it as edit and CP the new one in its place. Else, add. 
if [ -f $PROXY_INSTALL_DIR/$PROXY_IMAGE ]; then
  p4 edit -c $changelist $PROXY_INSTALL_DIR/$PROXY_IMAGE
else
  p4 add -c $changelist $PROXY_IMAGE
fi
#Edit property file
p4 edit -c $changelist $INSTALL_BASE_DIR/$PROXY_PROP_DIR/$PROXY_PROP_FILE
#Update the version in the file
echo "Version found: `grep bookkeeper.version < $INSTALL_BASE_DIR/$PROXY_PROP_DIR/$PROXY_PROP_FILE`"
sed -i.backup "s/\s\{0,\}bookkeeper.version\s\{0,\}=.*$/bookkeeper.version=${CURRENT_VERSION}/" $INSTALL_BASE_DIR/$PROXY_PROP_DIR/$PROXY_PROP_FILE
echo "Updated version to: `grep bookkeeper.version < $INSTALL_BASE_DIR/$PROXY_PROP_DIR/$PROXY_PROP_FILE`"
cp $ARTIFACT_DIR/$PROXY_IMAGE $PROXY_INSTALL_DIR/$PROXY_IMAGE
ls -l $PROXY_INSTALL_DIR
echo "Submitting proxy changelist"
# Submit changelist of Proxy first. If it succeeds, then we can continue to sfstore. If not, halt. 

p4 submit -c $changelist
if [ "$?" -ne 0 ];then
   p4 remove $PROXY_IMAGE
   p4 remove $INSTALL_BASE_DIR/$PROXY_PROP_DIR/$PROXY_PROP_FILE
   p4 change -d $changelist
   echo "Failed to submit $PROXY_IMAGE. Cannot proceed with $SFSTORE_IMAGE check-in. Exiting." && exit -1
else
  #Remove back-up that we created since we successfully updated this. 
  rm $INSTALL_BASE_DIR/$PROXY_PROP_DIR/$PROXY_PROP_FILE.backup
fi
echo "Successfully submitted $PROXY_IMAGE to p4"


if [ -d $SFSTORE_INSTALL_DIR ];then
   cd $SFSTORE_INSTALL_DIR
else
   echo "ERROR: $SFSTORE_DEST_DIR not found. Something wrong." && exit -1
fi

#Swap PROXY_IMAGE with $SFSTORE_IMAGE
sed -i.backup "s/$PROXY_IMAGE/$SFSTORE_IMAGE/" $p4infile

# Create the changelist
changelist=`cat $p4infile | p4 change -i | awk '{print $2}'`
echo "Sfstore changelist>"$changelist"<"

#If it already exists, mark it as edit and CP the new one in its place. Else, add. 
if [ -f $SFSTORE_INSTALL_DIR/$SFSTORE_IMAGE ]; then
  p4 edit -c $changelist $SFSTORE_INSTALL_DIR/$SFSTORE_IMAGE
else
  p4 add -c $changelist $SFSTORE_IMAGE
fi
#Edit property file
p4 edit -c $changelist $INSTALL_BASE_DIR/$SFSTORE_PROP_DIR/$SFSTORE_PROP_FILE
#Update the version in the file
echo "Version found: `grep BOOKKEEPER_VERSION < $INSTALL_BASE_DIR/$SFSTORE_PROP_DIR/$SFSTORE_PROP_FILE`"
echo "Updating version in property file: $INSTALL_BASE_DIR/$SFSTORE_PROP_DIR/$SFSTORE_PROP_FILE"
sed -i.backup "s/\s\{0,\}BOOKKEEPER_VERSION\s\{0,\}=.*$/BOOKKEEPER_VERSION=${CURRENT_VERSION}/" $INSTALL_BASE_DIR/$SFSTORE_PROP_DIR/$SFSTORE_PROP_FILE
echo "Copying artifact from $ARTIFACT_DIR/$SFSTORE_IMAGE to $SFSTORE_INSTALL_DIR/$SFSTORE_IMAGE"
cp $ARTIFACT_DIR/$SFSTORE_IMAGE $SFSTORE_INSTALL_DIR/$SFSTORE_IMAGE
ls -l $SFSSTORE_INSTALL_DIR
echo "Submitting sfstore changelist"
p4 submit -c $changelist
if [ "$?" -ne 0 ];then
   echo "Failed to submit $SFSTORE_IMAGE. Cannot proceed with $SFSTORE_IMAGE check-in. Proceeding with script for clean-up."
   p4 remove $SFSTORE_IMAGE
   p4 remove $INSTALL_BASE_DIR/$SFSTORE_PROP_DIR/$SFSTORE_PROP_FILE
   p4 change -d $changelist
else
  rm $INSTALL_BASE_DIR/$SFSTORE_PROP_DIR/$SFSTORE_PROP_FILE.backup 
  echo "Successfully submitted $SFSTORE_IMAGE to p4"
fi

if [ -e $p4infile ]; then
  #Remove both p4infile and its backup. 
  rm $p4infile*
fi

if [ -e $p4infile ]; then
  echo "Could not delete $p4infile" && exit -1
fi

echo "Complete"
