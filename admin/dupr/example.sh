#! /bin/bash

SCRATCH_DIR=scratch

SOURCE_INPUT=in/source.tsv
SOURCE_INDEX_DIR=index/source
SOURCE_CHUNK_DIR=chunks/source
SOURCE_FIELDS=sourceId,scienceCcdExposureId,filterId,objectId,movingObjectId,procHistoryId,ra,raSigmaForDetection,raSigmaForWcs,decl,declSigmaForDetection,declSigmaForWcs,htmId20,xFlux,xFluxSigma,yFlux,yFluxSigma,raFlux,raFluxSigma,declFlux,declFluxSigma,xPeak,yPeak,raPeak,declPeak,xAstrom,xAstromSigma,yAstrom,yAstromSigma,raAstrom,raAstromSigma,declAstrom,declAstromSigma,raObject,declObject,taiMidPoint,taiRange,psfFlux,psfFluxSigma,apFlux,apFluxSigma,modelFlux,modelFluxSigma,petroFlux,petroFluxSigma,instFlux,instFluxSigma,nonGrayCorrFlux,nonGrayCorrFluxSigma,atmCorrFlux,atmCorrFluxSigma,apDia,Ixx,IxxSigma,Iyy,IyySigma,Ixy,IxySigma,psfIxx,psfIxxSigma,psfIyy,psfIyySigma,psfIxy,psfIxySigma,e1_SG,e1_SG_Sigma,e2_SG,e2_SG_Sigma,resolution_SG,shear1_SG,shear1_SG_Sigma,shear2_SG,shear2_SG_Sigma,sourceWidth_SG,sourceWidth_SG_Sigma,shapeFlag_SG,snr,chi2,sky,skySigma,extendedness,flux_Gaussian,flux_Gaussian_Sigma,flux_ESG,flux_ESG_Sigma,sersicN_SG,sersicN_SG_Sigma,radius_SG,radius_SG_Sigma,flux_flux_SG_Cov,flux_e1_SG_Cov,flux_e2_SG_Cov,flux_radius_SG_Cov,flux_sersicN_SG_Cov,e1_e1_SG_Cov,e1_e2_SG_Cov,e1_radius_SG_Cov,e1_sersicN_SG_Cov,e2_e2_SG_Cov,e2_radius_SG_Cov,e2_sersicN_SG_Cov,radius_radius_SG_Cov,radius_sersicN_SG_Cov,sersicN_sersicN_SG_Cov,flagForAssociation,flagForDetection,flagForWcs

OBJECT_INPUT=in/object.tsv
OBJECT_INDEX_DIR=index/object
OBJECT_CHUNK_DIR=chunks/object
OBJECT_FIELDS=objectId,iauId,ra_PS,ra_PS_Sigma,decl_PS,decl_PS_Sigma,radecl_PS_Cov,htmId20,ra_SG,ra_SG_Sigma,decl_SG,decl_SG_Sigma,radecl_SG_Cov,raRange,declRange,muRa_PS,muRa_PS_Sigma,muDecl_PS,muDecl_PS_Sigma,muRaDecl_PS_Cov,parallax_PS,parallax_PS_Sigma,canonicalFilterId,extendedness,varProb,earliestObsTime,latestObsTime,meanObsTime,flags,uNumObs,uExtendedness,uVarProb,uRaOffset_PS,uRaOffset_PS_Sigma,uDeclOffset_PS,uDeclOffset_PS_Sigma,uRaDeclOffset_PS_Cov,uRaOffset_SG,uRaOffset_SG_Sigma,uDeclOffset_SG,uDeclOffset_SG_Sigma,uRaDeclOffset_SG_Cov,uLnL_PS,uLnL_SG,uFlux_PS,uFlux_PS_Sigma,uFlux_ESG,uFlux_ESG_Sigma,uFlux_Gaussian,uFlux_Gaussian_Sigma,uTimescale,uEarliestObsTime,uLatestObsTime,uSersicN_SG,uSersicN_SG_Sigma,uE1_SG,uE1_SG_Sigma,uE2_SG,uE2_SG_Sigma,uRadius_SG,uRadius_SG_Sigma,uFlags,gNumObs,gExtendedness,gVarProb,gRaOffset_PS,gRaOffset_PS_Sigma,gDeclOffset_PS,gDeclOffset_PS_Sigma,gRaDeclOffset_PS_Cov,gRaOffset_SG,gRaOffset_SG_Sigma,gDeclOffset_SG,gDeclOffset_SG_Sigma,gRaDeclOffset_SG_Cov,gLnL_PS,gLnL_SG,gFlux_PS,gFlux_PS_Sigma,gFlux_ESG,gFlux_ESG_Sigma,gFlux_Gaussian,gFlux_Gaussian_Sigma,gTimescale,gEarliestObsTime,gLatestObsTime,gSersicN_SG,gSersicN_SG_Sigma,gE1_SG,gE1_SG_Sigma,gE2_SG,gE2_SG_Sigma,gRadius_SG,gRadius_SG_Sigma,gFlags,rNumObs,rExtendedness,rVarProb,rRaOffset_PS,rRaOffset_PS_Sigma,rDeclOffset_PS,rDeclOffset_PS_Sigma,rRaDeclOffset_PS_Cov,rRaOffset_SG,rRaOffset_SG_Sigma,rDeclOffset_SG,rDeclOffset_SG_Sigma,rRaDeclOffset_SG_Cov,rLnL_PS,rLnL_SG,rFlux_PS,rFlux_PS_Sigma,rFlux_ESG,rFlux_ESG_Sigma,rFlux_Gaussian,rFlux_Gaussian_Sigma,rTimescale,rEarliestObsTime,rLatestObsTime,rSersicN_SG,rSersicN_SG_Sigma,rE1_SG,rE1_SG_Sigma,rE2_SG,rE2_SG_Sigma,rRadius_SG,rRadius_SG_Sigma,rFlags,iNumObs,iExtendedness,iVarProb,iRaOffset_PS,iRaOffset_PS_Sigma,iDeclOffset_PS,iDeclOffset_PS_Sigma,iRaDeclOffset_PS_Cov,iRaOffset_SG,iRaOffset_SG_Sigma,iDeclOffset_SG,iDeclOffset_SG_Sigma,iRaDeclOffset_SG_Cov,iLnL_PS,iLnL_SG,iFlux_PS,iFlux_PS_Sigma,iFlux_ESG,iFlux_ESG_Sigma,iFlux_Gaussian,iFlux_Gaussian_Sigma,iTimescale,iEarliestObsTime,iLatestObsTime,iSersicN_SG,iSersicN_SG_Sigma,iE1_SG,iE1_SG_Sigma,iE2_SG,iE2_SG_Sigma,iRadius_SG,iRadius_SG_Sigma,iFlags,zNumObs,zExtendedness,zVarProb,zRaOffset_PS,zRaOffset_PS_Sigma,zDeclOffset_PS,zDeclOffset_PS_Sigma,zRaDeclOffset_PS_Cov,zRaOffset_SG,zRaOffset_SG_Sigma,zDeclOffset_SG,zDeclOffset_SG_Sigma,zRaDeclOffset_SG_Cov,zLnL_PS,zLnL_SG,zFlux_PS,zFlux_PS_Sigma,zFlux_ESG,zFlux_ESG_Sigma,zFlux_Gaussian,zFlux_Gaussian_Sigma,zTimescale,zEarliestObsTime,zLatestObsTime,zSersicN_SG,zSersicN_SG_Sigma,zE1_SG,zE1_SG_Sigma,zE2_SG,zE2_SG_Sigma,zRadius_SG,zRadius_SG_Sigma,zFlags,yNumObs,yExtendedness,yVarProb,yRaOffset_PS,yRaOffset_PS_Sigma,yDeclOffset_PS,yDeclOffset_PS_Sigma,yRaDeclOffset_PS_Cov,yRaOffset_SG,yRaOffset_SG_Sigma,yDeclOffset_SG,yDeclOffset_SG_Sigma,yRaDeclOffset_SG_Cov,yLnL_PS,yLnL_SG,yFlux_PS,yFlux_PS_Sigma,yFlux_ESG,yFlux_ESG_Sigma,yFlux_Gaussian,yFlux_Gaussian_Sigma,yTimescale,yEarliestObsTime,yLatestObsTime,ySersicN_SG,ySersicN_SG_Sigma,yE1_SG,yE1_SG_Sigma,yE2_SG,yE2_SG_Sigma,yRadius_SG,yRadius_SG_Sigma,yFlags,chunkId,subChunkId

# ===============================================================
# Generate duplication indexes for PT1.2 Source and Object tables
# ===============================================================


# Generate Object table duplication index. Input is a tab-separated
# table dump obtained via:
#
# mysql -u XXX -pYYY \
#       -h lsst10.ncsa.illinois.edu -D rplante_PT1_2_u_pt12prod_im2000 \
#       -B --quick --disable-column-names \
#       -e "SELECT * FROM Object;" > $OBJECT_INPUT
qserv_dup_index \
    --num-threads=2 \
    --merge-arity=64 \
    --block-size=16 \
    --htm-level=8 \
    --fields=$OBJECT_FIELDS \
    --primary-key=objectId \
    --partitioned-by=ra_PS,decl_PS \
    --delimiter=$'\t' \
    --index-dir=$OBJECT_INDEX_DIR \
    --scratch-dir=$SCRATCH_DIR \
    --input=$OBJECT_INPUT

# Generate Source table duplication index. Input is a tab-separated
# table dump obtained via:
#
# mysql -u XXX -pYYY \
#       -h lsst10.ncsa.illinois.edu -D rplante_PT1_2_u_pt12prod_im2000 \
#       -B --quick --disable-column-names \
#       -e "SELECT * FROM Source;" > $SOURCE_INPUT
bin/qserv_dup_index \
    --num-threads=4 \
    --merge-arity=64 \
    --block-size=32 \
    --htm-level=8 \
    --fields=$SOURCE_FIELDS \
    --primary-key=sourceId \
    --partitioned-by=raObject,declObject \
    --delimiter=$'\t' \
    --index-dir=$SOURCE_INDEX_DIR \
    --scratch-dir=$SCRATCH_DIR \
    --input=$SOURCE_INPUT

# =====================================================================================
# Generate data for all chunks overlapping the region 60 <= RA <= 72, -30 <= Dec <= -18
# =====================================================================================

# Object chunks
qserv_dup \
    --num-threads=4 \
    --block-size=32 \
    --fields=$OBJECT_FIELDS \
    --delimiter=$'\t' \
    --primary-key=objectId \
    --partitioned-by=ra_PS,decl_PS \
    --position=ra_SG,decl_SG \
    --num-stripes=85 --num-sub-stripes-per-stripe=12 --overlap=0.01667 \
    --ra-min=60 --ra-max=72 --dec-min=-30 --dec-max=-18 \
    --index-dir=$OBJECT_INDEX_DIR \
    --prefix=object \
    --chunk-dir=$OBJECT_CHUNK_DIR

# Source chunks
qserv_dup \
    --num-threads=8 \
    --block-size=32 \
    --fields=$SOURCE_FIELDS \
    --delimiter=$'\t' \
    --primary-key=sourceId \
    --secondary-sort-field=objectId \
    --foreign-key=objectId,$OBJECT_INDEX_DIR \
    --partitioned-by=raObject,declObject \
    --position=ra,decl \
    --position=raFlux,declFlux \
    --position=raPeak,declPeak \
    --position=raAstrom,declAstrom \
    --num-stripes=85 --num-sub-stripes-per-stripe=12 --overlap=0 \
    --ra-min=60 --ra-max=72 --dec-min=-30 --dec-max=-18 \
    --index-dir=$SOURCE_INDEX_DIR \
    --prefix=source \
    --chunk-dir=$SOURCE_CHUNK_DIR
 
