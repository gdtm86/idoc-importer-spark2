package com.cloudera.sa.bsci.matmas

import scala.collection.mutable

/**
 * Created by gmedasani on 4/25/17.
 */
object MaterialTableMappings {

  /*
 define and retrieve material master tables' primary keys
*/
  def getMaterialMasterPrimaryKeys():mutable.HashMap[String,List[String]]= {
    val mararawstaging_PrimaryKeys = List[String]("docnum")
    val mara_PrimaryKeys = List[String]("matnr")
    val marc_PrimaryKeys = List[String]("matnr","werks")
    val mard_PrimaryKeys = List[String]("matnr","werks","lgort")
    val mkal_PrimaryKeys = List[String]("matnr","werks","verid")
    val makt_PrimaryKeys = List[String]("matnr","spras")
    val mbew_PrimaryKeys = List[String]("matnr","bwkey")
    val marm_PrimaryKeys = List[String]("matnr","meinh")
    val mean_PrimaryKeys = List[String]("matnr","meinh","lfnum")
    val mlan_PrimaryKeys = List[String]("matnr","aland")
    val mlgn_PrimaryKeys = List[String]("matnr","lgnum")
    val mlgt_PrimaryKeys = List[String]("matnr","lgnum","lgtyp")
    val mvke_PrimaryKeys = List[String]("matnr","vkorg","vtweg")

    val materialMasterPrimaryKeys = new mutable.HashMap[String,List[String]]()
    materialMasterPrimaryKeys.put("mararawstaging",mararawstaging_PrimaryKeys)
    materialMasterPrimaryKeys.put("mara",mara_PrimaryKeys)
    materialMasterPrimaryKeys.put("marc",marc_PrimaryKeys)
    materialMasterPrimaryKeys.put("mard",mard_PrimaryKeys)
    materialMasterPrimaryKeys.put("mkal",mkal_PrimaryKeys)
    materialMasterPrimaryKeys.put("makt",makt_PrimaryKeys)
    materialMasterPrimaryKeys.put("mbew",mbew_PrimaryKeys)
    materialMasterPrimaryKeys.put("marm",marm_PrimaryKeys)
    materialMasterPrimaryKeys.put("mean",mean_PrimaryKeys)
    materialMasterPrimaryKeys.put("mlan",mlan_PrimaryKeys)
    materialMasterPrimaryKeys.put("mlgn",mlgn_PrimaryKeys)
    materialMasterPrimaryKeys.put("mlgt",mlgt_PrimaryKeys)
    materialMasterPrimaryKeys.put("mvke",mvke_PrimaryKeys)
    materialMasterPrimaryKeys
  }

  /*
    define and retrieve material master tables' valid fields
 */
  def getMaterialMasterValidFields():mutable.HashMap[String,List[String]] = {
    val mararawstaging_ValidFields = List[String]("DOCNUM","SEGMENT","TABNAM","MANDT","DOCREL","STATUS","DIRECT","OUTMOD","IDOCTYP","CIMTYP","MESTYP","STDMES","SNDPOR","SNDPRT","SNDPRN","RCVPOR","RCVPRT","RCVPRN","CREDAT","CRETIM","SERIAL","IDOCJSON")
    val mara_ValidFields = List[String]("MATNR","ERSDA","ERNAM","LAEDA","AENAM","VPSTA","PSTAT","LVORM","MTART","MBRSH","MATKL","BISMT","MEINS","BSTME","ZEINR","ZEIAR","ZEIVR","ZEIFO","AESZN","BLATT","BLANZ","FERTH","FORMT","GROES","WRKST","NORMT","LABOR","EKWSL","BRGEW","NTGEW","GEWEI","VOLUM","VOLEH","BEHVO","RAUBE","TEMPB","DISST","TRAGR","STOFF","SPART","KUNNR","EANNR","WESCH","BWVOR","BWSCL","SAISO","ETIAR","ETIFO","ENTAR","EAN11","NUMTP","LAENG","BREIT","HOEHE","MEABM","PRDHA","AEKLK","CADKZ","QMPUR","ERGEW","ERGEI","ERVOL","ERVOE","GEWTO","VOLTO","VABME","KZREV","KZKFG","XCHPF","VHART","FUELG","STFAK","MAGRV","BEGRU","DATAB","LIQDT","SAISJ","PLGTP","MLGUT","EXTWG","SATNR","ATTYP","KZKUP","KZNFM","PMATA","MSTAE","MSTAV","MSTDE","MSTDV","TAKLV","RBNRM","MHDRZ","MHDHB","MHDLP","INHME","INHAL","VPREH","ETIAG","INHBR","CMETH","CUOBF","KZUMW","KOSCH","SPROF","NRFHG","MFRPN","MFRNR","BMATN","MPROF","KZWSM","SAITY","PROFL","IHIVI","ILOOS","SERLV","KZGVH","XGCHP","KZEFF","COMPL","IPRKZ","RDMHD","PRZUS","MTPOS_MARA","BFLME","MATFI","CMREL","BBTYP","SLED_BBD","GTIN_VARIANT","GENNR","RMATP","GDS_RELEVANT","WEORA","HUTYP_DFLT","PILFERABLE","WHSTC","WHMATGR","HNDLCODE","HAZMAT","HUTYP","TARE_VAR","MAXC","MAXC_TOL","MAXL","MAXB","MAXH","MAXDIM_UOM","HERKL","MFRGR","QQTIME","QQTIMEUOM","QGRP","SERIAL","PS_SMARTFORM","LOGUNIT","CWQREL","CWQPROC","CWQTOLGR","ADPROF","IPMIPPRODUCT","ALLOW_PMAT_IGNO","MEDIUM","ANIMAL_ORIGIN","TEXTILE_COMP_IND","ANP","BEV1_LULEINH","BEV1_LULDEGRP","BEV1_NESTRUCCAT","DSD_SL_TOLTYP","DSD_SV_CNT_GRP","DSD_VC_GROUP","VSO_R_TILT_IND","VSO_R_STACK_IND","VSO_R_BOT_IND","VSO_R_TOP_IND","VSO_R_STACK_NO","VSO_R_PAL_IND","VSO_R_PAL_OVR_D","VSO_R_PAL_OVR_W","VSO_R_PAL_B_HT","VSO_R_PAL_MIN_H","VSO_R_TOL_B_HT","VSO_R_NO_P_GVH","VSO_R_QUAN_UNIT","VSO_R_KZGVH_IND","PACKCODE","DG_PACK_STATUS","MCOND","RETDELC","LOGLEV_RETO","NSNID","IMATN","PICNUM","BSTAT","COLOR_ATINN","SIZE1_ATINN","SIZE2_ATINN","COLOR","SIZE1","SIZE2","FREE_CHAR","CARE_CODE","BRAND_ID","FIBER_CODE1","FIBER_PART1","FIBER_CODE2","FIBER_PART2","FIBER_CODE3","FIBER_PART3","FIBER_CODE4","FIBER_PART4","FIBER_CODE5","FIBER_PART5","FASHGRD","ZZWRKST","ZZPRODUCT","ZZLANCON","PRDHA2","ZZDRUGIND","ZZCMI","ZZSRAIAPPROVAL")
    val marc_ValidFields = List[String]("MATNR","WERKS","PSTAT","LVORM","BWTTY","XCHAR","MMSTA","MMSTD","MAABC","KZKRI","EKGRP","AUSME","DISPR","DISMM","DISPO","KZDIE","PLIFZ","WEBAZ","PERKZ","AUSSS","DISLS","BESKZ","SOBSL","MINBE","EISBE","BSTMI","BSTMA","BSTFE","BSTRF","MABST","LOSFX","SBDKZ","LAGPR","ALTSL","KZAUS","AUSDT","NFMAT","KZBED","MISKZ","FHORI","PFREI","FFREI","RGEKZ","FEVOR","BEARZ","RUEZT","TRANZ","BASMG","DZEIT","MAXLZ","LZEIH","KZPRO","GPMKZ","UEETO","UEETK","UNETO","WZEIT","ATPKZ","VZUSL","HERBL","INSMK","SPROZ","QUAZT","SSQSS","MPDAU","KZPPV","KZDKZ","WSTGH","PRFRQ","NKMPR","UMLMC","LADGR","XCHPF","USEQU","LGRAD","AUFTL","PLVAR","OTYPE","OBJID","MTVFP","PERIV","KZKFK","VRVEZ","VBAMG","VBEAZ","LIZYK","BWSCL","KAUTB","KORDB","STAWN","HERKL","HERKR","EXPME","MTVER","PRCTR","TRAME","MRPPP","SAUFT","FXHOR","VRMOD","VINT1","VINT2","VERKZ","STLAL","STLAN","PLNNR","APLAL","LOSGR","SOBSK","FRTME","LGPRO","DISGR","KAUSF","QZGTP","QMATV","TAKZT","RWPRO","COPAM","ABCIN","AWSLS","SERNP","CUOBJ","STDPD","SFEPR","XMCNG","QSSYS","LFRHY","RDPRF","VRBMT","VRBWK","VRBDT","VRBFK","AUTRU","PREFE","PRENC","PRENO","PREND","PRENE","PRENG","ITARK","SERVG","KZKUP","STRGR","CUOBV","LGFSB","SCHGT","CCFIX","EPRIO","QMATA","RESVP","PLNTY","UOMGR","UMRSL","ABFAC","SFCPF","SHFLG","SHZET","MDACH","KZECH","MEGRU","MFRGR","VKUMC","VKTRW","KZAGL","FVIDK","FXPRU","LOGGR","FPRFM","GLGMG","VKGLG","INDUS","MOWNR","MOGRU","CASNR","GPNUM","STEUC","FABKZ","MATGR","VSPVB","DPLFS","DPLPU","DPLHO","MINLS","MAXLS","FIXLS","LTINC","COMPL","CONVT","SHPRO","AHDIS","DIBER","KZPSP","OCMPF","APOKZ","MCRUE","LFMON","LFGJA","EISLO","NCOST","ROTATION_DATE","UCHKZ","UCMAT","BWESB","VSO_R_PKGRP","VSO_R_LANE_NUM","VSO_R_PAL_VEND","VSO_R_FORK_DIR","IUID_RELEVANT","IUID_TYPE","UID_IEA","CONS_PROCG","GI_PR_TIME","MULTIPLE_EKGRP","REF_SCHEMA","MIN_TROC","MAX_TROC","TARGET_STOCK","ZADAT","ZRNUM","ZEDAT")
    val mard_ValidFields = List[String]("MATNR","WERKS","LGORT","PSTAT","LVORM","LFGJA","LFMON","SPERR","LABST","UMLME","INSME","EINME","SPEME","RETME","VMLAB","VMUML","VMINS","VMEIN","VMSPE","VMRET","KZILL","KZILQ","KZILE","KZILS","KZVLL","KZVLQ","KZVLE","KZVLS","DISKZ","LSOBS","LMINB","LBSTF","HERKL","EXPPG","EXVER","LGPBE","KLABS","KINSM","KEINM","KSPEM","DLINL","PRCTL","ERSDA","VKLAB","VKUML","LWMKB","BSKRF","MDRUE","MDJIN")
    val mkal_ValidFields = List[String]("MATNR","WERKS","VERID","BDATU","ADATU","STLAL","STLAN","PLNTY","PLNNR","ALNAL","BESKZ","SOBSL","LOSGR","MDV01","MDV02","TEXT1","EWAHR","VERTO","SERKZ","BSTMI","BSTMA","RGEKZ","ALORT","PLTYG","PLNNG","ALNAG","PLTYM","PLNNM","ALNAM","CSPLT","MATKO","ELPRO","PRVBE","PRFG_F","PRDAT","MKSP","PRFG_R","PRFG_G","PRFG_S","UCMAT","PPEGUID")
    val makt_ValidFields = List[String]("MATNR","SPRAS","MAKTX","MAKTG")
    val mbew_ValidFields = List[String]("MATNR","BWKEY","BWTAR","LVORM","LBKUM","SALK3","VPRSV","VERPR","STPRS","PEINH","BKLAS","SALKV","VMKUM","VMSAL","VMVPR","VMVER","VMSTP","VMPEI","VMBKL","VMSAV","VJKUM","VJSAL","VJVPR","VJVER","VJSTP","VJPEI","VJBKL","VJSAV","LFGJA","LFMON","BWTTY","STPRV","LAEPR","ZKPRS","ZKDAT","TIME_STAMP","BWPRS","BWPRH","VJBWS","VJBWH","VVJSL","VVJLB","VVMLB","VVSAL","ZPLPR","ZPLP1","ZPLP2","ZPLP3","ZPLD1","ZPLD2","ZPLD3","PPERZ","PPERL","PPERV","KALKZ","KALKL","KALKV","KALSC","XLIFO","MYPOL","BWPH1","BWPS1","ABWKZ","PSTAT","KALN1","KALNR","BWVA1","BWVA2","BWVA3","VERS1","VERS2","VERS3","HRKFT","KOSGR","PPRDZ","PPRDL","PPRDV","PDATZ","PDATL","PDATV","EKALR","VPLPR","MLMAA","MLAST","LPLPR","VKSAL","HKMAT","SPERW","KZIWL","WLINL","ABCIW","BWSPA","LPLPX","VPLPX","FPLPX","LBWST","VBWST","FBWST","EKLAS","QKLAS","MTUSE","MTORG","OWNPR","XBEWM","BWPEI","MBRUE","OKLAS","OIPPINV")
    val marm_ValidFields = List[String]("MATNR","MEINH","UMREZ","UMREN","EANNR","EAN11","NUMTP","LAENG","BREIT","HOEHE","MEABM","VOLUM","VOLEH","BRGEW","GEWEI","MESUB","ATINN","MESRT","XFHDW","XBEWW","KZWSO","MSEHI","BFLME_MARM","GTIN_VARIANT","NEST_FTR","MAX_STACK","CAPAUSE","TY2TQ")
    val mean_ValidFields = List[String]("MATNR","MEINH","LFNUM","EAN11","EANTP","HPEAN")
    val mlan_ValidFields = List[String]("MATNR","ALAND","TAXM1","TAXM2","TAXM3","TAXM4","TAXM5","TAXM6","TAXM7","TAXM8","TAXM9","TAXIM")
    val mlgn_ValidFields = List[String]("MATNR","LGNUM","LVORM","LGBKZ","LTKZE","LTKZA","LHMG1","LHMG2","LHMG3","LHME1","LHME2","LHME3","LETY1","LETY2","LETY3","LVSME","KZZUL","BLOCK","KZMBF","BSSKZ","MKAPV","BEZME","PLKPT","VOMEM","L2SKR")
    val mlgt_ValidFields = List[String]("MATNR","LGNUM","LGTYP","LVORM","LGPLA","LPMAX","LPMIN","MAMNG","NSMNG","KOBER","RDMNG")
    val mvke_ValidFields = List[String]("MATNR","VKORG","VTWEG","LVORM","VERSG","BONUS","PROVG","SKTOF","VMSTA","VMSTD","AUMNG","LFMNG","EFMNG","SCMNG","SCHME","VRKME","MTPOS","DWERK","PRODH","PMATN","KONDM","KTGRM","MVGR1","MVGR2","MVGR3","MVGR4","MVGR5","SSTUF","PFLKS","LSTFL","LSTVZ","LSTAK","LDVFL","LDBFL","LDVZL","LDBZL","VDVFL","VDBFL","VDVZL","VDBZL","PRAT1","PRAT2","PRAT3","PRAT4","PRAT5","PRAT6","PRAT7","PRAT8","PRAT9","PRATA","RDPRF","MEGRU","LFMAX","RJART","PBIND","VAVME","MATKC","PVMSO","BEV1_EMLGRP","BEV1_EMDRCKSPL","BEV1_RPBEZME","BEV1_RPSNS","BEV1_RPSFA","BEV1_RPSKI","BEV1_RPSCO","BEV1_RPSSO","PLGTP")

    val materialMasterValidFields = new mutable.HashMap[String,List[String]]()
    materialMasterValidFields.put("mararawstaging",mararawstaging_ValidFields)
    materialMasterValidFields.put("mara",mara_ValidFields)
    materialMasterValidFields.put("marc",marc_ValidFields)
    materialMasterValidFields.put("mard",mard_ValidFields)
    materialMasterValidFields.put("mkal",mkal_ValidFields)
    materialMasterValidFields.put("makt",makt_ValidFields)
    materialMasterValidFields.put("mbew",mbew_ValidFields)
    materialMasterValidFields.put("marm",marm_ValidFields)
    materialMasterValidFields.put("mean",mean_ValidFields)
    materialMasterValidFields.put("mlan",mlan_ValidFields)
    materialMasterValidFields.put("mlgn",mlgn_ValidFields)
    materialMasterValidFields.put("mlgt",mlgt_ValidFields)
    materialMasterValidFields.put("mvke",mvke_ValidFields)
    materialMasterValidFields
  }

  /*
    define and retrieve valid segments for material master idoc
 */
  def getMaterialMasterSegments() = {
    //val structureSegments = List("E1MFHMM","E1MPGDM","E1MPOPM","E1MPRWM","E1MVEGM","E1MVEUM") //Structures aren't defined
    val validSegments = List("EDI_DC40","E1MARAM","E1MARA1","ZEI_MARAM","E1MAKTM", "E1MARCM","E1MBEWM",
      "E1MVKEM","E1MARDM","E1MLANM","E1MKALM","E1MARMM","E1MEANM","E1MLGNM","E1MLGTM")
    validSegments
  }

  /*
    define and retrieve segment to table mapping for material master
 */
  def getSegmentToTableMapping(): mutable.HashMap[String,String] = {
    val materialTableMapping = new mutable.HashMap[String,String]()
    materialTableMapping.put("EDI_DC40","mararawstaging")
    materialTableMapping.put("E1MARAM","mara")
    materialTableMapping.put("E1MARA1","mara")
    materialTableMapping.put("ZEI_MARAM","mara")
    materialTableMapping.put("E1MAKTM","makt")
    materialTableMapping.put("E1MARCM","marc")
    materialTableMapping.put("E1MBEWM","mbew")
    materialTableMapping.put("E1MVKEM","mvke")
    materialTableMapping.put("E1MARDM","mard")
    materialTableMapping.put("E1MLANM","mlan")
    materialTableMapping.put("E1MKALM","mkal")
    materialTableMapping.put("E1MARMM","marm")
    materialTableMapping.put("E1MEANM","mean")
    materialTableMapping.put("E1MLGNM","mlgn")
    materialTableMapping.put("E1MLGTM","mlgt")
    materialTableMapping.put("E1MFHMM","mfhm")
    materialTableMapping.put("E1MPGDM","mpgd")
    materialTableMapping.put("E1MPOPM","mpop")
    materialTableMapping.put("E1MPRWM","mprw")
    materialTableMapping.put("E1MVEGM","mveg")
    materialTableMapping.put("E1MVEUM","mveu")
    materialTableMapping
  }

  /*
  define and retrieve segment to table mapping for material master
  */
  def getSegmentChildren():mutable.HashMap[String,List[String]] = {
    val matmasChildrenMappig = new mutable.HashMap[String,List[String]]()
    matmasChildrenMappig.put("E1MARCM",List("E1MARDM","E1MKALM"))
    matmasChildrenMappig.put("E1MARMM",List("E1MEANM"))
    matmasChildrenMappig.put("E1MLGNM",List("E1MLGTM"))
    matmasChildrenMappig
  }


}
