package com.ctrip.hotel.search.facade.soa2;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.hotel.product.search.cache.common.util.DateCommonUtils;
import com.ctrip.hotel.search.business.HelpBusiness;
import com.ctrip.hotel.search.business.common.GlobalFunctions;
import com.ctrip.hotel.search.business.factory.lazy.LazyModeUtil;
import com.ctrip.hotel.search.business.utility.DateTimeUtil;
import com.ctrip.hotel.search.common.constant.CustomerTagKeyConst;
import com.ctrip.hotel.search.common.constant.TimeCostConst;
import com.ctrip.hotel.search.common.context.ContextItemKey;
import com.ctrip.hotel.search.common.context.ContextKeyUtils;
import com.ctrip.hotel.search.common.context.ContextStrKey;
import com.ctrip.hotel.search.common.context.LocalContext;
import com.ctrip.hotel.search.common.qmq.QMQCommon;
import com.ctrip.hotel.search.common.qmq.QMQConst;
import com.ctrip.hotel.search.common.utility.CommonUtils;
import com.ctrip.hotel.search.conf.SearchServiceConfig;
import com.ctrip.hotel.search.core.entity.TimeCostEntity;
import com.ctrip.hotel.search.core.entity.VmsReadRedisTimeCostEntity;
import com.ctrip.hotel.search.core.toolutils.logger.LogHelper;
import com.ctrip.hotel.search.core.util.ConfigurationTypeConst;
import com.ctrip.hotel.search.core.util.TaskBitFlagWriter;
import com.ctrip.hotel.search.core.util.number.NumberConverter;
import com.ctrip.hotel.search.core.util.serialize.JsonUtil;
import com.ctrip.hotel.search.core.util.string.StringCollectionUtils;
import com.ctrip.hotel.search.core.util.string.StringCommonUtils;
import com.ctrip.hotel.search.facade.log.FacadeLogHelper;
import com.ctrip.hotel.search.facade.utility.MessageUtil;
import com.ctrip.hotel.search.internalentity.InterfaceRedisMetric;
import com.ctrip.hotel.search.internalentity.common.CatLogStruct;
import com.ctrip.hotel.search.internalentity.enums.HotelTagFilterType;
import com.ctrip.hotel.search.internalentity.searchresponse.DebugEntity;
import com.ctrip.hotel.search.soa.util.soa.CServiceUtil;
import com.ctrip.hotel.search.soa.util.sscontext.SSContext;
import com.ctrip.soa.hotel.product.searchservice.v1.*;
import com.ctriposs.baiji.rpc.server.HttpRequestContext;
import com.ctriposs.baiji.rpc.server.HttpRequestWrapper;
import com.dianping.cat.Cat;
import com.dianping.cat.message.spi.MessageTree;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * @author dp
 * @version release1009
 * ExtendFunction
 */
@Component
public class FacadeExtendFunction {
    private static final String trueValue = "T";
    private static final String falseValue = "F";
    /**
     * ???????????????????????????????????????,logcat ???
     */
    public static String[] loggerRoomPriceMessageKeys =
            new String[]{"roomprice_0", "roomprice_1", "roomprice_2", "roomprice_3", "roomprice_4", "roomprice_5", "roomprice_6", "roomprice_7", "roomprice_8", "roomprice_9"};
    @Autowired
    LazyModeUtil lazyModeUtil;
    @Autowired
    private DateTimeUtil dateTimeUtil;
    /**
     * ??????RequestXML???clog???
     *
     * @param request
     */
    public void logRequestToClog(SearchHotelDataRequest request, String clientAppId, String requestType) {
        if (request == null) {
            return;
        }
        String xml = "";
        if (SearchServiceConfig.getInstance().getIsGettingRequest()) {
            try {
                // ???????????????
                GettingRequestHelper.getRequestV2(request, clientAppId, requestType, 0);
            } catch (RuntimeException ex) {
                FacadeLogHelper.getInstance().logErrorWithoutMail(ex.getMessage(), ex);
            }
        }
        // ??????UID?????????DEBUG??????Request XML
        if (!SearchServiceConfig.getInstance().getAllowWriteUIDToHttpContext() && !StringCommonUtils.isNullOrEmpty(request.getPublicSearchParameter().getUid())
                && SearchServiceConfig.getInstance().getDebugUserIds().contains(request.getPublicSearchParameter().getUid())) {
            if (StringCommonUtils.isNullOrEmpty(xml)) {
                // xml = Soa1SerializeUtility.serializeToXML(request,
                // SearchServiceConfig.getInstance().DefaultSearchRequestMemoryStreamBufferSize);
                xml = JsonUtil.stringify(request);
            }
            HashMap<String, String> addinfo = new HashMap<String, String>();
            addinfo.put("logtype", "rq");
            addinfo.put("uid", request.getPublicSearchParameter().getUid());
            addinfo.put("cid", CServiceUtil.getClientAppId());
            HelpBusiness.getLogger().info(MessageUtil.FacadeExtendFunction0 + request.getPublicSearchParameter().getUid(), xml, addinfo);
            if (SearchServiceConfig.getInstance().getEnableLogRequestToCat()) {
                SSContext.setRequestText(xml);
                SSContext.setRequestType("uid");
            }
        }
    }

    public void logRequestToClog(com.ctrip.hotel.search.internalentity.searchrequest.SearchHotelDataRequest request, String appid, String requestType) {
        if (request == null) {
            return;
        }
        String xml = "";
        // ??????UID?????????DEBUG??????Request XML
        if (!SearchServiceConfig.getInstance().getAllowWriteUIDToHttpContext() &&
                StringCommonUtils.isNullOrEmpty(request.getPublicSearchParameter().getUid())
                && SearchServiceConfig.getInstance().getDebugUserIds().contains(request.getPublicSearchParameter().getUid())) {
            if (StringCommonUtils.isNullOrEmpty(xml)) {
                xml = JsonUtil.stringify(request);
            }
            HashMap<String, String> addinfo = new HashMap<String, String>();
            addinfo.put("logtype", "rq");
            addinfo.put("uid", request.getPublicSearchParameter().getUid());
            addinfo.put("cid", CServiceUtil.getClientAppId());
            HelpBusiness.getLogger().info(MessageUtil.FacadeExtendFunction10 + request.getPublicSearchParameter().getUid(), xml, addinfo);
            if (SearchServiceConfig.getInstance().getEnableLogRequestToCat()) {
                SSContext.setRequestText(xml);
                SSContext.setRequestType("uid");
            }
        }
    }

    public void collectFieldsForCat(SearchHotelDataRequest request, int soaVersion, String clientId, String clientIp, boolean redirect) {
        collectFieldsForCat(request, soaVersion, clientId, clientIp, redirect, 0);
    }

    public void collectFieldsForCat(SearchHotelDataRequest request, int soaVersion, String clientId, String clientIp) {
        collectFieldsForCat(request, soaVersion, clientId, clientIp, false, 0);
    }

    public void collectFieldsForCat(HotelCalendarSearchRequest request, int soaVersion, String clientId, String clientIp) {
        collectFieldsForCat(request, soaVersion, clientId, clientIp, false, 0);
    }

    public void collectFieldsForCat(SearchHotelDataRequest request, int soaVersion, String clientId, String clientIp, boolean redirect,
                                    int shardIndex) {
        // ????????????
        if (!SearchServiceConfig.getInstance().getLogCat()) {
            return;
        }
        if (request == null || request.getPublicSearchParameter() == null) {
            return;
        }
        /* ?????????Dictionary????????????????????????????????????!!! */
        HashMap<String, String> catLogFields = new HashMap<>(120);
        HashMap<String, String> indexTags = catLogFields;
        // ???????????????????????????ThreadLocal??????
        HttpRequestWrapper cRequest = null;
        if (HttpRequestContext.getInstance() != null) {
            cRequest = HttpRequestContext.getInstance().request();
        }
        FacadeCommonUtils.setCatLogFields(cRequest, catLogFields);


        HashMap<String, String> catLogFieldsV2 = new HashMap<>(120);
        HashMap<String, String> indexTagsV2 = catLogFieldsV2;
        FacadeCommonUtils.setCatLogFieldsV2(cRequest, catLogFieldsV2);

        PublicParametersEntity args = request.getPublicSearchParameter();
        // #region ??????????????????
        // ?????????????????????
        indexTags.put("starlist", args.getStarList());
        // ?????????????????????
        indexTags.put("zonelist", args.getZoneList());
        // ????????????????????????
        indexTags.put("locationlist", args.getLocationList());
        // ???????????????????????????
        indexTags.put("channellistset", args.getChannelList());
        // ?????????????????????ID??????
        int hotelListCount = StringCollectionUtils.getSplitLength(args.getHotelList());
        indexTags.put("hotellist", String.valueOf(hotelListCount));
        indexTags.put("hotelcount307", String.valueOf(hotelListCount));
        // ???????????????????????????
        indexTags.put("hotelname", StringCommonUtils.isNullOrEmpty(args.getHotelName()) ? "0" : "1");
        indexTags.put("keyword", StringCommonUtils.isNullOrWhiteSpace(args.getKeyWord()) ? "0" : "1");
        // ????????????????????????????????????
        indexTags.put("contractsceneid", String.valueOf(request.getSearchTypeEntity().getContractSceneID()));
        // ?????????????????????????????????
        int hotelcount = request.getSearchTypeEntity().getHotelCount();
        if (SearchServiceConfig.getInstance().getFixOverflowNumForSearchServiceLog() && hotelcount < 0) {
            hotelcount = 0;
        }

        if (SSContext.getBusinessConfig().getEnableRecorduserbizsceneToCK()) {
            if (request.getPublicSearchParameter().getCustomerTagList() != null &&
                    request.getPublicSearchParameter().getCustomerTagList().size() > 0) {
                for (CustomerTagEntity tag : request.getPublicSearchParameter().getCustomerTagList()) {
                    if (tag == null || tag.getTagKey() == null || StringCommonUtils.isNullOrWhiteSpace(tag.getTagValue())) {
                        continue;
                    }
                    if (StringCommonUtils.isEquals(CustomerTagKeyConst.User_Biz_Scene, tag.getTagKey())) {
                        if (!StringCommonUtils.isNullOrWhiteSpace(tag.getTagValue())) {
                            indexTags.put("user_biz_scene", String.valueOf(tag.getTagValue()));
                            break;
                        }
                    }
                }
            }
        }


        indexTags.put("hotelcount", String.valueOf(hotelcount));
        // ??????????????????????????????
        if (request.getSearchTypeEntity().getIsGetCache() != null && request.getSearchTypeEntity().getIsGetCache() == true) {
            indexTags.put("isgetcache", trueValue);
        } else {
            indexTags.put("isgetcache", falseValue);
        }
        // ?????????????????????
        int pageIndex = request.getSearchTypeEntity().getPageIndex();
        if (SearchServiceConfig.getInstance().getFixOverflowNumForSearchServiceLog() && pageIndex < 0) {
            pageIndex = 0;
        }
        indexTags.put("pageindex", String.valueOf(pageIndex));
        // ????????????????????????????????????
        // ???????????????Type
        indexTags.put("searchdatatype", request.getSearchTypeEntity().getSearchDataType() == null ? SearchDataType.OffLineIntlSearch.name() : request.getSearchTypeEntity().getSearchDataType().toString());
        indexTags.put("searchtype", request.getSearchTypeEntity().getSearchType() == null ? SearchType.StandardSearch.name() : request.getSearchTypeEntity().getSearchType().toString());
        // ?????????????????????ID
        int cityId = args.getCity();
        if (SearchServiceConfig.getInstance().getFixOverflowNumForSearchServiceLog() && cityId < 0) {
            cityId = 0;
        }
        indexTags.put("city", String.valueOf(cityId));
        // ????????????????????????
        if (args.getCheckInDate() != null) {
            indexTags.put("checkindate", DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(args.getCheckInDate()));
        }
        // ????????????????????????
        if (args.getCheckOutDate() != null) {
            indexTags.put("checkoutdate", DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(args.getCheckOutDate()));
        }
        // ?????????????????????
        indexTags.put("star", String.valueOf(args.getStar()));
        // ???????????????????????????
        indexTags.put("ordername", args.getOrderName() == null ? StringUtils.EMPTY : args.getOrderName().toString());
        // ???????????????????????????
        indexTags.put("ordertype", args.getOrderType() == null ? StringUtils.EMPTY : args.getOrderType().toString());
        // ????????????????????????????????????
        indexTags.put("usegivenchannelhotelscore", args.getUseGivenChannelHotelScore() == null ? GivenChannelHotelScore.None.name() : args.getUseGivenChannelHotelScore().toString());
        // ???????????????????????????
        indexTags.put("iscashback", args.getIsCashBack() ? trueValue : falseValue);
        // ????????????????????????????????????
        indexTags.put("showshadowrooms", args.getShowShadowRooms() ? trueValue : falseValue);
        // ????????????
        indexTags.put("breakfastnumfilter", StringCommonUtils.isNullOrWhiteSpace(args.getBreakfastNum()) ? falseValue : trueValue);
        indexTags.put("breakfastnum", StringCommonUtils.isNullOrWhiteSpace(args.getBreakfastNum()) ? "-1" : args.getBreakfastNum());
        // ?????????
        indexTags.put("fgtopp", args.getFgToPP() ? trueValue : falseValue);
        // ???????????????????????????
        indexTags.put("isrequesttravelmoney", args.getIsRequestTravelMoney() ? trueValue : falseValue);
        // ???????????????
        indexTags.put("onlyfgprice", args.getOnlyFGPrice() ? trueValue : falseValue);
        indexTags.put("onlyppprice", args.getOnlyPPPrice() ? trueValue : falseValue);
        // ??????
        indexTags.put("breakfast", args.getBreakFast() ? trueValue : falseValue);
        // ??????????????????(????????????608189)
        indexTags.put("propertyvalueidlist", (args.getPropertyValueIDList() != null) ? args.getPropertyValueIDList() : "");
        indexTags.put("propertycodes", (args.getPropertyCodeList() != null) ? args.getPropertyCodeList() : "");
        // ??????
        indexTags.put("ispromoteroomtype", (args.getIsPromoteRoomType() != null) ? args.getIsPromoteRoomType() : "");
        // ??????
        indexTags.put("room", String.valueOf(args.getRoom()));
        // ??????
        if (request.getPersonaInfoEntity() != null) {
            String scenario = request.getPersonaInfoEntity().getScenario();
            indexTags.put("scenario", scenario != null ? scenario : "");
        }
        if (args.getHotelTagsFilter() != null) {
            // ????????????
            indexTags.put("tagsfreecancelationroom", args.getHotelTagsFilter().contains(HotelTagFilterType.FreeCancelationRoom.getValue()) ? trueValue : falseValue);
            // ????????????
            indexTags.put("tagsctripchoice", args.getHotelTagsFilter().contains(HotelTagFilterType.CtripChoice.getValue()) ? trueValue : falseValue);
            // ????????????
            indexTags.put("tagsctripselfrun", args.getHotelTagsFilter().contains(HotelTagFilterType.CtripSelfRun.getValue()) ? trueValue : falseValue);
            // ????????????
            indexTags.put("tagslowpricesupplier", args.getHotelTagsFilter().contains(HotelTagFilterType.LowPriceSupplier.getValue()) ? trueValue : falseValue);
        }
        // ????????????
        indexTags.put("pricearea", !StringCommonUtils.isNullOrWhiteSpace(args.getMultiPriceSection()) ? args.getMultiPriceSection() : "");
        // ????????????
        if (request.getMapSearchEntity() != null) {
            indexTags.put("dotx", String.valueOf(request.getMapSearchEntity().getDotX()));
            indexTags.put("dotx2", String.valueOf(request.getMapSearchEntity().getDotX2()));
            indexTags.put("doty", String.valueOf(request.getMapSearchEntity().getDotY()));
            indexTags.put("doty2", String.valueOf(request.getMapSearchEntity().getDotY2()));
            indexTags.put("maxlat", String.valueOf(request.getMapSearchEntity().getMaxLat()));
            indexTags.put("minlat", String.valueOf(request.getMapSearchEntity().getMinLat()));
            indexTags.put("maxlng", String.valueOf(request.getMapSearchEntity().getMaxLng()));
            indexTags.put("minlng", String.valueOf(request.getMapSearchEntity().getMinLng()));
            indexTags.put("radius", String.valueOf(request.getMapSearchEntity().getRadius()));
        } else {
            indexTags.put("radius", "0");
        }
        indexTags.put("sortstagedistance", String.valueOf(args.getSortStageDistance()));

        if (args.getSwitchList() == null || args.getSwitchList().size() == 0) {
            indexTags.put("switchlist", "0");
            indexTags.put("dbname", "searchHotelData");
        } else {
            indexTags.put("switchlist", StringCommonUtils.join(args.getSwitchList(), ","));
            if (args.getSwitchList().contains(202204)){
                indexTags.put("dbname", "searchHotelCount");
            } else if (args.getSwitchList().contains(202205)) {
                indexTags.put("dbname", "searchHotelList");
            }
            else{
                indexTags.put("dbname", "searchHotelData");
            }

        }
        if (args.getPlatformPromotionIDList() != null) {
            indexTags.put("platformPromotionIdCount",String.valueOf(StringCollectionUtils.splitConvertHashSet(args.getPlatformPromotionIDList()).size()));
        }

        if (request.getFacilityEntity() != null) {
            HotelFacilitiesEntity facility = request.getFacilityEntity();
            // ????????????
            if (facility.getLowPrice() > 0 || facility.getHighPrice() > 0) {
                indexTags.put("pricearea", facility.getLowPrice() + "," + facility.getHighPrice());
            } else {
                if (indexTags.containsKey("pricearea") && trueValue.equals(indexTags.get("pricearea"))) {
                } else {
                    indexTags.put("pricearea", "");
                }
            }
            // ??????
            indexTags.put("hotelbrandlist", facility.getHotelBrand() > 0 ? String.valueOf(facility.getHotelBrand()) : facility.getHotelBrandList());
            // ??????
            indexTags.put("hotelmgrgrouplist", (facility.getHotelMgrGroupList() != null) ? facility.getHotelMgrGroupList() : "");
            // ?????????
            indexTags.put("metro", String.valueOf(facility.getMetro()));
            // ????????????
            indexTags.put("isjustifyconfirm", StringUtils.trimToEmpty(facility.getIsJustifyConfirm()));
            // ??????
            if (facility.getIsCanReserve() != null && facility.getIsCanReserve().booleanValue() == true) {
                indexTags.put("iscanreserve", trueValue);
            } else {
                indexTags.put("iscanreserve", falseValue);
            }
        }
        // ??????????????????
        indexTags.put("userlevel", args.getUserProfileLevel());
        // #endregion
        // #region ?????????????????????
        // ????????????
        indexTags.put("cityname", null);
        // ????????????
        indexTags.put("interval", "0");
        // ??????????????????
        indexTags.put("ismapsearch", "F");
        // ?????????????????????
        indexTags.put("hitsearchlogic", "0");
        // ?????????????????????????????????????????????
        indexTags.put("rs_hotels", "0");
        // ???????????????????????????????????????
        indexTags.put("responseallhotelcount", "0");
        // ???????????????????????????ErrorCode

        indexTags.put("errorcode", "");

        // FixSubHotel
        indexTags.put("fixsubhotel", args.getFixSubHotel() ? trueValue : falseValue);
        // ticketgiftsnum
        indexTags.put("ticketgiftsnum", "0");
        // optionalhotelnum
        indexTags.put("optionalhotelnum", "0");
        // floatingpricenum
        indexTags.put("floatingpricenum", "0");
        // ticketusercouponnum
        indexTags.put("ticketusercouponnum", "0");
        // redirect
        indexTags.put("redirect", redirect ? trueValue : falseValue);
        // shardindex
        indexTags.put("shardindex", String.valueOf(shardIndex));
        if (SearchServiceConfig.getInstance().getEnableMatchSpiderLazyLogic()) {
            // ??????????????????CC_SPD(0:???CC_SPD,1:CC_SPD)
            indexTags.put("spider", "0");
        }
        // ????????????IdList
        if (args.getIdList() != null && args.getIdList().getHotelIdList() != null && args.getIdList().getHotelIdList().size() > 0) {
            indexTags.put("soaversion", "1");
        } else {
            indexTags.put("soaversion", "0");
        }
        // ?????????
        indexTags.put("soaversion", String.valueOf(soaVersion));
        // ?????????
        indexTags.put("singlehotel", hotelListCount == 1 ? trueValue : falseValue);
        // ????????????
        indexTags.put("range", String.valueOf(DateCommonUtils.getDaysBetween(args.getCheckInDate().getTime(), args.getCheckOutDate().getTime())));
        // ???????????????????????????
        Date localDateTime = new Date();
        if (SSContext.getBusinessConfig().getEnableTimeConvert()) {
            // ????????????
            localDateTime = dateTimeUtil.covertLocalDateByRequest(localDateTime, args);
        }
        indexTags.put("checkinoffset", String.valueOf(DateCommonUtils.getDaysBetween(localDateTime, args.getCheckInDate().getTime())));

        if (SSContext.getBusinessConfig().getEnableRecordCheckinOffsetDaysToInt()) {
            indexTags.put("checkin_offset_days", String.valueOf(DateCommonUtils.getDaysBetween(new java.util.Date(), args.getCheckInDate().getTime())));
        }

        // ClintId
        indexTags.put("clientid", clientId);
        indexTagsV2.put("clientid", clientId);// indexTagsV2-??????1
        // ClientIP
        indexTags.put("clientip", clientIp);
        // Format
        if (SearchServiceConfig.getInstance().getIsEnableLogToCatWithRQFormat()) {
            indexTags.put("format", soaVersion == 2 ? CServiceUtil.getClientFormat() : "XML");
        } else {
            ContractFormat cf = request.getSearchTypeEntity().getContractFormat();
            indexTags.put("format", soaVersion == 2 ? cf == null ? ContractFormat.X.name() : cf.name() : "X");
        }
        // ????????????(ws/ws3/drws/drws3)
        indexTags.put("cluster", ConfigurationTypeConst.getCluster());
        // ????????????(ws_jq|ws_oy/shard_jq|shard_oy/router_jq/router_oy)
        indexTags.put("group", SSContext.getBusinessConfig().getServiceGroup());
        // UID
        indexTags.put("uid", (args.getUid() != null) ? args.getUid() : "");
        // LogId=vid+pageindex ???????????????????????????
        indexTags.put("tracelogid", (args.getLogId() != null) ? args.getLogId() : "");
        // excludeticketgift
        indexTags.put("excludeticketgift", args.getExcludeTicketGift() ? trueValue : falseValue);
        // args.CouponParameter.
        if (args.getCouponParameter() != null) {
            indexTags.put("couponcount", args.getCouponParameter().getCount().toString());
            indexTags.put("excludenotdisplay", args.getCouponParameter().getExcludeNotDisplay() == null || args.getCouponParameter().getExcludeNotDisplay() ? trueValue : falseValue);
        } else {
            indexTags.put("couponcount", "0");
            indexTags.put("excludenotdisplay", falseValue);
        }
        // ??????????????????????????????ID
        if (hotelListCount == 1) {
            indexTags.put("zybusedcheck01", args.getHotelList());
            indexTags.put("hotellist307", args.getHotelList());
        } else if (args.getSwitchList() != null && (args.getSwitchList().contains(100003) || args.getSwitchList().contains(100005))) {
            indexTags.put("zybusedcheck01", args.getHotelList());
            indexTags.put("hotellist307", args.getHotelList());
        } else {
            indexTags.put("zybusedcheck01", "0");
            indexTags.put("hotellist307", "0");
        }
        indexTags.put("zybusedcheck02", "T");
        indexTags.put("isjavacluster", "T");
        indexTags.put("specialtagids", args.getSpecialTagIDs() != null ? args.getSpecialTagIDs() : "");
        indexTags.put("hotelspecialtagids", args.getHotelSpecialTagIDs() != null ? args.getHotelSpecialTagIDs() : "");
        indexTags.put("ratePlanRQ", SSContext.isRatePlanRQ() ? "T" : "F");
        indexTags.put("batchid", args.getBatchID());
        indexTags.put("batchseq", String.valueOf(args.getBatchSeq()));
        indexTags.put("invoicetargettype", String.valueOf(args.getInvoiceTargetType()));
        if (!StringCommonUtils.isNullOrEmpty(args.getFilterTicketPlatformPromotionIDList())
                && !StringCommonUtils.isNullOrEmpty(args.getPlatformPromotionIDList())
                && !StringCommonUtils.isEquals(args.getFilterTicketPlatformPromotionIDList(), args.getPlatformPromotionIDList())) {
            // ???????????????????????????????????????????????????????????????????????????????????????????????????????????????
            indexTags.put("differentcouponidparams", "T");
        }
        indexTags.put("servicecode", CServiceUtil.getAppServiceCode() != null ? CServiceUtil.getAppServiceCode() : "");
        int aid = args.getAllianceID();
        if (SearchServiceConfig.getInstance().getFixOverflowNumForSearchServiceLog() && aid < 0) {
            aid = 0;
        }
        indexTags.put("aid", String.valueOf(aid));

        if (SearchServiceConfig.getInstance().getRecordEsUpStreamAppID()) {
            indexTags.put("upstreamappid", "0");
        }

        if (SearchServiceConfig.getInstance().getEsRecordServiceType()) {
            // ????????????????????????
            indexTags.put("servicetype", "1");
        }
    }

    public void collectFieldsForCat(HotelCalendarSearchRequest request, int soaVersion, String clientId, String clientIp, boolean redirect,
                                    int shardIndex) {
        // ????????????
        if (!SearchServiceConfig.getInstance().getLogCat()) {
            return;
        }
        if (request == null || request.getPublicSearchParameter() == null) {
            return;
        }
        /* ?????????Dictionary????????????????????????????????????!!! */
        HashMap<String, String> catLogFields = new HashMap<>(120);
        HashMap<String, String> indexTags = catLogFields;
        // ???????????????????????????ThreadLocal??????
        HttpRequestWrapper cRequest = null;
        if (HttpRequestContext.getInstance() != null) {
            cRequest = HttpRequestContext.getInstance().request();
        }
        FacadeCommonUtils.setCatLogFields(cRequest, catLogFields);


        HashMap<String, String> catLogFieldsV2 = new HashMap<>(120);
        HashMap<String, String> indexTagsV2 = catLogFieldsV2;
        FacadeCommonUtils.setCatLogFieldsV2(cRequest, catLogFieldsV2);

        PublicParametersEntity args = request.getPublicSearchParameter();
        // #region ??????????????????
        // ?????????????????????
        indexTags.put("starlist", args.getStarList());
        // ?????????????????????
        indexTags.put("zonelist", args.getZoneList());
        // ????????????????????????
        indexTags.put("locationlist", args.getLocationList());
        // ???????????????????????????
        indexTags.put("channellistset", args.getChannelList());
        // ?????????????????????ID??????
        int hotelListCount = StringCollectionUtils.getSplitLength(args.getHotelList());
        indexTags.put("hotellist", String.valueOf(hotelListCount));
        indexTags.put("hotelcount307", String.valueOf(hotelListCount));
        // ???????????????????????????
        indexTags.put("hotelname", StringCommonUtils.isNullOrEmpty(args.getHotelName()) ? "0" : "1");
        indexTags.put("keyword", StringCommonUtils.isNullOrWhiteSpace(args.getKeyWord()) ? "0" : "1");
        // ????????????????????????????????????
        indexTags.put("contractsceneid", String.valueOf(request.getSearchTypeEntity().getContractSceneID()));
        // ?????????????????????????????????
        int hotelcount = request.getSearchTypeEntity().getHotelCount();
        if (SearchServiceConfig.getInstance().getFixOverflowNumForSearchServiceLog() && hotelcount < 0) {
            hotelcount = 0;
        }
        indexTags.put("hotelcount", String.valueOf(hotelcount));
        // ??????????????????????????????
        if (request.getSearchTypeEntity().getIsGetCache() != null && request.getSearchTypeEntity().getIsGetCache() == true) {
            indexTags.put("isgetcache", trueValue);
        } else {
            indexTags.put("isgetcache", falseValue);
        }
        // ?????????????????????
        int pageIndex = request.getSearchTypeEntity().getPageIndex();
        if (SearchServiceConfig.getInstance().getFixOverflowNumForSearchServiceLog() && pageIndex < 0) {
            pageIndex = 0;
        }
        indexTags.put("pageindex", String.valueOf(pageIndex));
        // ????????????????????????????????????
        // ???????????????Type
        indexTags.put("searchdatatype", request.getSearchTypeEntity().getSearchDataType() == null ? SearchDataType.OffLineIntlSearch.name() : request.getSearchTypeEntity().getSearchDataType().toString());
        indexTags.put("searchtype", request.getSearchTypeEntity().getSearchType() == null ? SearchType.StandardSearch.name() : request.getSearchTypeEntity().getSearchType().toString());
        // ?????????????????????ID
        int cityId = args.getCity();
        if (SearchServiceConfig.getInstance().getFixOverflowNumForSearchServiceLog() && cityId < 0) {
            cityId = 0;
        }
        indexTags.put("city", String.valueOf(cityId));
        // ????????????????????????
        if (args.getCheckInDate() != null) {
            indexTags.put("checkindate", DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(args.getCheckInDate()));
        }
        // ????????????????????????
        if (args.getCheckOutDate() != null) {
            indexTags.put("checkoutdate", DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(args.getCheckOutDate()));
        }
        // ?????????????????????
        indexTags.put("star", String.valueOf(args.getStar()));
        // ???????????????????????????
        indexTags.put("ordername", args.getOrderName() == null ? StringUtils.EMPTY : args.getOrderName().toString());
        // ???????????????????????????
        indexTags.put("ordertype", args.getOrderType() == null ? StringUtils.EMPTY : args.getOrderType().toString());
        // ????????????????????????????????????
        indexTags.put("usegivenchannelhotelscore", args.getUseGivenChannelHotelScore() == null ? GivenChannelHotelScore.None.name() : args.getUseGivenChannelHotelScore().toString());
        // ???????????????????????????
        indexTags.put("iscashback", args.getIsCashBack() ? trueValue : falseValue);
        // ????????????????????????????????????
        indexTags.put("showshadowrooms", args.getShowShadowRooms() ? trueValue : falseValue);
        // ????????????
        indexTags.put("breakfastnumfilter", StringCommonUtils.isNullOrWhiteSpace(args.getBreakfastNum()) ? falseValue : trueValue);
        indexTags.put("breakfastnum", StringCommonUtils.isNullOrWhiteSpace(args.getBreakfastNum()) ? "-1" : args.getBreakfastNum());
        // ?????????
        indexTags.put("fgtopp", args.getFgToPP() ? trueValue : falseValue);
        // ???????????????????????????
        indexTags.put("isrequesttravelmoney", args.getIsRequestTravelMoney() ? trueValue : falseValue);
        // ???????????????
        indexTags.put("onlyfgprice", args.getOnlyFGPrice() ? trueValue : falseValue);
        indexTags.put("onlyppprice", args.getOnlyPPPrice() ? trueValue : falseValue);
        // ??????
        indexTags.put("breakfast", args.getBreakFast() ? trueValue : falseValue);
        // ??????????????????(????????????608189)
        indexTags.put("propertyvalueidlist", (args.getPropertyValueIDList() != null) ? args.getPropertyValueIDList() : "");
        indexTags.put("propertycodes", (args.getPropertyCodeList() != null) ? args.getPropertyCodeList() : "");
        // ??????
        indexTags.put("ispromoteroomtype", (args.getIsPromoteRoomType() != null) ? args.getIsPromoteRoomType() : "");
        // ??????
        indexTags.put("room", String.valueOf(args.getRoom()));
        // ??????
        if (request.getPersonaInfoEntity() != null) {
            String scenario = request.getPersonaInfoEntity().getScenario();
            indexTags.put("scenario", scenario != null ? scenario : "");
        }
        if (args.getHotelTagsFilter() != null) {
            // ????????????
            indexTags.put("tagsfreecancelationroom", args.getHotelTagsFilter().contains(HotelTagFilterType.FreeCancelationRoom.getValue()) ? trueValue : falseValue);
            // ????????????
            indexTags.put("tagsctripchoice", args.getHotelTagsFilter().contains(HotelTagFilterType.CtripChoice.getValue()) ? trueValue : falseValue);
            // ????????????
            indexTags.put("tagsctripselfrun", args.getHotelTagsFilter().contains(HotelTagFilterType.CtripSelfRun.getValue()) ? trueValue : falseValue);
            // ????????????
            indexTags.put("tagslowpricesupplier", args.getHotelTagsFilter().contains(HotelTagFilterType.LowPriceSupplier.getValue()) ? trueValue : falseValue);
        }
        // ????????????
        indexTags.put("pricearea", !StringCommonUtils.isNullOrWhiteSpace(args.getMultiPriceSection()) ? args.getMultiPriceSection() : "");
        // ????????????
        indexTags.put("radius", "0");
        indexTags.put("sortstagedistance", String.valueOf(args.getSortStageDistance()));

        if (args.getSwitchList() == null || args.getSwitchList().size() == 0) {
            indexTags.put("switchlist", "0");
        } else {
            indexTags.put("switchlist", StringCommonUtils.join(args.getSwitchList(), ","));
        }

        if (request.getFacilityEntity() != null) {
            HotelFacilitiesEntity facility = request.getFacilityEntity();
            // ????????????
            if (facility.getLowPrice() > 0 || facility.getHighPrice() > 0) {
                indexTags.put("pricearea", facility.getLowPrice() + "," + facility.getHighPrice());
            } else {
                if (indexTags.containsKey("pricearea") && trueValue.equals(indexTags.get("pricearea"))) {
                } else {
                    indexTags.put("pricearea", "");
                }
            }
            // ??????
            indexTags.put("hotelbrandlist", facility.getHotelBrand() > 0 ? String.valueOf(facility.getHotelBrand()) : facility.getHotelBrandList());
            // ??????
            indexTags.put("hotelmgrgrouplist", (facility.getHotelMgrGroupList() != null) ? facility.getHotelMgrGroupList() : "");
            // ?????????
            indexTags.put("metro", String.valueOf(facility.getMetro()));
            // ????????????
            indexTags.put("isjustifyconfirm", StringUtils.trimToEmpty(facility.getIsJustifyConfirm()));
            // ??????
            if (facility.getIsCanReserve() != null && facility.getIsCanReserve().booleanValue() == true) {
                indexTags.put("iscanreserve", trueValue);
            } else {
                indexTags.put("iscanreserve", falseValue);
            }
        }
        // ??????????????????
        indexTags.put("userlevel", args.getUserProfileLevel());
        // #endregion
        // #region ?????????????????????
        // ????????????
        indexTags.put("cityname", null);
        // ????????????
        indexTags.put("interval", "0");
        // ??????????????????
        indexTags.put("ismapsearch", "F");
        // ?????????????????????
        indexTags.put("hitsearchlogic", "0");
        // ??????????????????????????????
        indexTags.put("dbname", "searchHotelCalendar");
        // ?????????????????????????????????????????????
        indexTags.put("rs_hotels", "0");
        // ???????????????????????????????????????
        indexTags.put("responseallhotelcount", "0");
        // ???????????????????????????ErrorCode
        indexTags.put("errorcode", "");
        // FixSubHotel
        indexTags.put("fixsubhotel", args.getFixSubHotel() ? trueValue : falseValue);
        // ticketgiftsnum
        indexTags.put("ticketgiftsnum", "0");
        // optionalhotelnum
        indexTags.put("optionalhotelnum", "0");
        // floatingpricenum
        indexTags.put("floatingpricenum", "0");
        // ticketusercouponnum
        indexTags.put("ticketusercouponnum", "0");
        // redirect
        indexTags.put("redirect", redirect ? trueValue : falseValue);
        // shardindex
        indexTags.put("shardindex", String.valueOf(shardIndex));
        if (SearchServiceConfig.getInstance().getEnableMatchSpiderLazyLogic()) {
            // ??????????????????CC_SPD(0:???CC_SPD,1:CC_SPD)
            indexTags.put("spider", "0");
        }
        // ????????????IdList
        if (args.getIdList() != null && args.getIdList().getHotelIdList() != null && args.getIdList().getHotelIdList().size() > 0) {
            indexTags.put("soaversion", "1");
        } else {
            indexTags.put("soaversion", "0");
        }
        // ?????????
        indexTags.put("soaversion", String.valueOf(soaVersion));
        // ?????????
        indexTags.put("singlehotel", hotelListCount == 1 ? trueValue : falseValue);
        // ????????????
        indexTags.put("range", String.valueOf(DateCommonUtils.getDaysBetween(args.getCheckInDate().getTime(), args.getCheckOutDate().getTime())));
        // ???????????????????????????
        indexTags.put("checkinoffset", String.valueOf(DateCommonUtils.getDaysBetween(new java.util.Date(), args.getCheckInDate().getTime())));
        if (SSContext.getBusinessConfig().getEnableRecordCheckinOffsetDaysToInt()) {
            indexTags.put("checkin_offset_days", String.valueOf(DateCommonUtils.getDaysBetween(new java.util.Date(), args.getCheckInDate().getTime())));
        }


        // ClintId
        indexTags.put("clientid", clientId);
        indexTagsV2.put("clientid", clientId);
        // ClientIP
        indexTags.put("clientip", clientIp);
        // Format
        if (SearchServiceConfig.getInstance().getIsEnableLogToCatWithRQFormat()) {
            indexTags.put("format", soaVersion == 2 ? CServiceUtil.getClientFormat() : "XML");
        } else {
            ContractFormat cf = request.getSearchTypeEntity().getContractFormat();
            indexTags.put("format", soaVersion == 2 ? cf == null ? ContractFormat.X.name() : cf.name() : "X");
        }
        // ????????????(ws/ws3/drws/drws3)
        indexTags.put("cluster", ConfigurationTypeConst.getCluster());
        // ????????????(ws_jq|ws_oy/shard_jq|shard_oy/router_jq/router_oy)
        indexTags.put("group", SSContext.getBusinessConfig().getServiceGroup());
        // UID
        indexTags.put("uid", (args.getUid() != null) ? args.getUid() : "");
        // LogId=vid+pageindex ???????????????????????????
        indexTags.put("tracelogid", (args.getLogId() != null) ? args.getLogId() : "");
        // excludeticketgift
        indexTags.put("excludeticketgift", args.getExcludeTicketGift() ? trueValue : falseValue);
        // args.CouponParameter.
        if (args.getCouponParameter() != null) {
            indexTags.put("couponcount", args.getCouponParameter().getCount().toString());
            indexTags.put("excludenotdisplay", args.getCouponParameter().getExcludeNotDisplay() == null || args.getCouponParameter().getExcludeNotDisplay() ? trueValue : falseValue);
        } else {
            indexTags.put("couponcount", "0");
            indexTags.put("excludenotdisplay", falseValue);
        }
        // ??????????????????????????????ID
        if (hotelListCount == 1) {
            indexTags.put("zybusedcheck01", args.getHotelList());
            indexTags.put("hotellist307", args.getHotelList());
        } else if (args.getSwitchList() != null && (args.getSwitchList().contains(100003) || args.getSwitchList().contains(100005))) {
            indexTags.put("zybusedcheck01", args.getHotelList());
            indexTags.put("hotellist307", args.getHotelList());
        } else {
            indexTags.put("zybusedcheck01", "0");
            indexTags.put("hotellist307", "0");
        }
        indexTags.put("zybusedcheck02", "T");
        indexTags.put("isjavacluster", "T");
        indexTags.put("specialtagids", args.getSpecialTagIDs() != null ? args.getSpecialTagIDs() : "");
        indexTags.put("hotelspecialtagids", args.getHotelSpecialTagIDs() != null ? args.getHotelSpecialTagIDs() : "");
        indexTags.put("ratePlanRQ", SSContext.isRatePlanRQ() ? "T" : "F");
        indexTags.put("batchid", args.getBatchID());
        indexTags.put("batchseq", String.valueOf(args.getBatchSeq()));
        indexTags.put("invoicetargettype", String.valueOf(args.getInvoiceTargetType()));
        if (!StringCommonUtils.isNullOrEmpty(args.getFilterTicketPlatformPromotionIDList())
                && !StringCommonUtils.isNullOrEmpty(args.getPlatformPromotionIDList())
                && !StringCommonUtils.isEquals(args.getFilterTicketPlatformPromotionIDList(), args.getPlatformPromotionIDList())) {
            // ???????????????????????????????????????????????????????????????????????????????????????????????????????????????
            indexTags.put("differentcouponidparams", "T");
        }
        indexTags.put("servicecode", CServiceUtil.getAppServiceCode() != null ? CServiceUtil.getAppServiceCode() : "");
        int aid = args.getAllianceID();
        if (SearchServiceConfig.getInstance().getFixOverflowNumForSearchServiceLog() && aid < 0) {
            aid = 0;
        }
        indexTags.put("aid", String.valueOf(aid));

        if (SearchServiceConfig.getInstance().getRecordEsUpStreamAppID()) {
            indexTags.put("upstreamappid", "0");
        }

        if (SearchServiceConfig.getInstance().getEsRecordServiceType()) {
            // ????????????????????????
            indexTags.put("servicetype", "1");
        }
    }

    public void logToCat(SearchHotelDataRequest searchHotelDataRequest, long elapsedMilliseconds, CatLogStruct responseInfo, DebugEntity debugEntity, String hitLogicIds,
                         String cacheMode, String errorCode) {
        logToCat(searchHotelDataRequest, elapsedMilliseconds, responseInfo, debugEntity, hitLogicIds, cacheMode, errorCode, null, 0);
    }

    public void logToCat(SearchHotelDataRequest searchHotelDataRequest, long elapsedMilliseconds, CatLogStruct responseInfo, DebugEntity debugEntity, String hitLogicIds,
                         String cacheMode) {
        logToCat(searchHotelDataRequest, elapsedMilliseconds, responseInfo, debugEntity, hitLogicIds, cacheMode, null, null, 0);
    }

    public void logToCat(SearchHotelDataRequest searchHotelDataRequest, long elapsedMilliseconds, CatLogStruct responseInfo, DebugEntity debugEntity, String hitLogicIds) {
        logToCat(searchHotelDataRequest, elapsedMilliseconds, responseInfo, debugEntity, hitLogicIds, "", null, null, 0);
    }

    public void logToCat(SearchHotelDataRequest searchHotelDataRequest, long elapsedMilliseconds, CatLogStruct responseInfo, DebugEntity debugEntity) {
        logToCat(searchHotelDataRequest, elapsedMilliseconds, responseInfo, debugEntity, "", "", null, null, 0);
    }

    public void logToCat(SearchHotelDataRequest searchHotelDataRequest, long elapsedMilliseconds, CatLogStruct responseInfo, DebugEntity debugEntity, String hitLogicIds,
                         String cacheMode, String errorCode, String notCanReserveCode) {
        logToCat(searchHotelDataRequest, elapsedMilliseconds, responseInfo, debugEntity, hitLogicIds, cacheMode, errorCode, notCanReserveCode, 0);
    }

    public void logToCat(HotelCalendarSearchRequest hotelCalendarSearchRequest, long elapsedMilliseconds, CatLogStruct responseInfo, DebugEntity debugEntity, String hitLogicIds,
                         String cacheMode, String errorCode, String notCanReserveCode) {
        logToCat(hotelCalendarSearchRequest, elapsedMilliseconds, responseInfo, debugEntity, hitLogicIds, cacheMode, errorCode, notCanReserveCode, 0);
    }

    private void logToCat(SearchHotelDataRequest searchHotelDataRequest, long elapsedMilliseconds, CatLogStruct responseInfo, DebugEntity debugEntity, String hitLogicIds,
                          String cacheMode, String errorCode, String notCanReserveCode, int shardIndex) {
        if (!SearchServiceConfig.getInstance().getLogCat()) {
            return;
        }
        HttpRequestWrapper cRequest = null;
        if (HttpRequestContext.getInstance() != null) {
            cRequest = HttpRequestContext.getInstance().request();
        }
        if (cRequest == null || searchHotelDataRequest == null || searchHotelDataRequest.getPublicSearchParameter() == null || FacadeCommonUtils.getCatLogFields(cRequest) == null) {
            return;
        }
        PublicParametersEntity args = searchHotelDataRequest.getPublicSearchParameter();
        Map<String, String> indexedTags = FacadeCommonUtils.getCatLogFields(cRequest);
        if (indexedTags == null) {
            indexedTags = new HashMap<>();
        }
        /**
         * ??????!!!????????????????????????indexedTags.Add??? ????????????????????????ExtendFunction.cs/CollectFieldsForCat????????????,?????????Dictionary????????????????????????????????????!!! by*
         * jwyuan@ctrip.com
         */
        if (responseInfo != null) {
            if (responseInfo.getResponseAllHotelCount() >= 0) {
                // ?????????????????????????????????????????????
                indexedTags.put("rs_hotels", String.valueOf(responseInfo.getResponseHotelCount()));
                if (searchHotelDataRequest.getSearchTypeEntity().getHotelCount() > 0) {
                    indexedTags.put("fillrate", String.valueOf(CommonUtils.round2FractionalDigits(((double) responseInfo.getResponseHotelCount() / (double) searchHotelDataRequest.getSearchTypeEntity().getHotelCount()), 2)));
                }
                // ???????????????????????????????????????
                indexedTags.put("responseallhotelcount", String.valueOf(responseInfo.getResponseAllHotelCount()));
                // ???????????????
                indexedTags.put("minprice", String.valueOf(responseInfo.getMinPrice()));
                // ?????????????????????
                indexedTags.put("minpriceroom", String.valueOf(responseInfo.getMinPriceRoom()));
                // ????????????????????????
                indexedTags.put("vendorid", String.valueOf(responseInfo.getVendorId()));
                // ???????????????????????????????????????????????????
                indexedTags.put("computeroomnum", String.valueOf(responseInfo.getComputeRoomCount()));
                // ?????????????????????????????????????????????????????????????????????
                indexedTags.put("computestoproomnum", String.valueOf(responseInfo.getComputeStopRoomCount()));
            }

            // ?????????????????????Country
            if (SearchServiceConfig.getInstance().getEnableRecordCountryID()
                    && responseInfo.getCountryId() > 0) {
                indexedTags.put("country", String.valueOf(responseInfo.getCountryId()));

            } else {
                // ?????????????????????Country
                indexedTags.put("country", String.valueOf(responseInfo.getCountryId()));
            }


            // ??????????????????iscanreserve
            indexedTags.put("requestlocal", (responseInfo.getOnlyRequestLocal() ? "T" : "F"));
            indexedTags.put("ticketgiftsnum", String.valueOf(responseInfo.getTicketGiftsNum()));
            indexedTags.put("optionalhotelnum", String.valueOf(responseInfo.getOptionalHotelNum()));
            indexedTags.put("floatingpricenum", String.valueOf(responseInfo.getFloatingPriceNum()));
            indexedTags.put("ticketusercouponnum", String.valueOf(responseInfo.getTicketUserCouponNum()));
            indexedTags.put("resetroomdinfonum", String.valueOf(responseInfo.getResetRoomDInfoCount()));
            indexedTags.put("shadowroomfiltertype", String.valueOf(responseInfo.getShadowRoomFilterType()));
            indexedTags.put("shadowdiscountfilter", String.valueOf(responseInfo.getShadowDiscountFilterType()));
            indexedTags.put("meta_tolerance", String.valueOf(responseInfo.getHasMetaTolerance() ? 1 : 0));
            indexedTags.put("tolerance_diffprice", String.valueOf(responseInfo.getToleranceDiffPrice()));
            indexedTags.put("tolerance_reason", String.valueOf(responseInfo.getShadowToleranceReason()));
            indexedTags.put("competion_info", String.valueOf(responseInfo.getCompetitionInfo()));
            indexedTags.put("shadowroomcount", String.valueOf(responseInfo.getShadowRoomCount()));
            indexedTags.put("personpriceroomnum", String.valueOf(responseInfo.getMultiPersonPriceRoomNum()));
            indexedTags.put("hotelavailrs", responseInfo.getHotelAvailSubHotelList());
            indexedTags.put("hotelavailsuccesscount", String.valueOf(responseInfo.getHotelAvailSuccessCount()));
            indexedTags.put("hotelavailtimeoutcount", String.valueOf(responseInfo.getHotelAvailTimeoutCount()));
            indexedTags.put("hotelavailtotalcount", String.valueOf(responseInfo.getHotelAvailTotalCount()));
            indexedTags.put("directbatchstatus", responseInfo.getDirectBatchStatus());
            indexedTags.put("interval_htlavail", String.valueOf(responseInfo.getInterval_htlavail()));
            indexedTags.put("interval_shadow", String.valueOf(responseInfo.getInterval_shadow()));
            indexedTags.put("interval_ibufilter", String.valueOf(responseInfo.getInterval_ibufilter()));
            indexedTags.put("interval_makeprice", String.valueOf(responseInfo.getInterval_makeprice()));
            indexedTags.put("makeprice_detail", responseInfo.getMakeprice_detail());
            indexedTags.put("interval_realtime", String.valueOf(responseInfo.getInterval_realtime()));
            indexedTags.put("interval_realtimepricing", String.valueOf(responseInfo.getInterval_realtimepricing()));
            indexedTags.put("interval_realtimepricing_total", String.valueOf(responseInfo.getInterval_realtimepricing_total()));
            indexedTags.put("count_realtimepricing", String.valueOf(responseInfo.getCount_realtimepricing()));
            indexedTags.put("filter_realtime", responseInfo.getFilter_realtime());
            indexedTags.put("interval_pricingprofit", String.valueOf(responseInfo.getInterval_pricingprofit()));
            indexedTags.put("interval_pricechangeblack", String.valueOf(responseInfo.getInterval_pricechangeblack()));
            indexedTags.put("isovsshadowallroomsoa", responseInfo.getHasOvsAllRoomSOA() ? "T" : "F");
            indexedTags.put("calcbestcoupon", args.getIsCalcRoomTicketAmountForUserCoupon() ? "T" : "F");
            indexedTags.put("hotelshadows", responseInfo.getShadow_RoomDetailList());
            indexedTags.put("hotelshadowminprices", responseInfo.getShadow_MinPriceRooms());
            indexedTags.put("allmarklogic", responseInfo.getAllMarkLogic());
            indexedTags.put("hotelshadowmisshotels", responseInfo.getShadow_MissHotels());
            indexedTags.put("shadowavailtime", String.valueOf(responseInfo.getShadowAvailTime()));
            indexedTags.put("roomsinglestr", responseInfo.getRoomSingleStr());
            indexedTags.put("clearandmappingtime", String.valueOf(responseInfo.getClearAndMappingTime()));
            indexedTags.put("hotelminpriceresetzerocount", String.valueOf(responseInfo.getHotelMinPriceResetZeroCount()));
            // recommend
            indexedTags.put("recommendcomputeroomcount", String.valueOf(responseInfo.getRecommendComputeRoomCount()));
            indexedTags.put("recommendcomputehotelcount", String.valueOf(responseInfo.getRecommendComputeHotelCount()));
            indexedTags.put("recommendhotelcount", String.valueOf(responseInfo.getRecommendHotelCount()));
            // ?????????????????????
            indexedTags.put("canbookroomnum", String.valueOf(responseInfo.getCanBookRoomNum()));
            indexedTags.put("notcanbookroomnum", String.valueOf(responseInfo.getNotCanBookRoomNum()));
            indexedTags.put("canbookhotelnum", String.valueOf(responseInfo.getCanBookHotelNum()));
            indexedTags.put("notcanbookhotelnum", String.valueOf(responseInfo.getNotCanBookHotelNum()));
            indexedTags.put("queryscence", responseInfo.getQueryScence());
            indexedTags.put(CustomerTagKeyConst.IBUPosID, responseInfo.getIbuPosId());
            indexedTags.put(CustomerTagKeyConst.CommissionRatePricingChannel, String.valueOf(responseInfo.getCommissionRatePricingChannel()));
            indexedTags.put("queryroomcount", String.valueOf((responseInfo.getRoomIdList() == null ? 0 : responseInfo.getRoomIdList().size())));
            if (responseInfo.getHotelListVisitHotelShard()) {
                indexedTags.put("prefetchdata", "1");
            } else if (searchHotelDataRequest.getPublicSearchParameter() != null
                    && searchHotelDataRequest.getPublicSearchParameter().getSwitchList() != null
                    && searchHotelDataRequest.getPublicSearchParameter().getSwitchList().contains(100003)) {
                indexedTags.put("prefetchdata", "2");
            } else if (searchHotelDataRequest.getPublicSearchParameter() != null
                    && searchHotelDataRequest.getPublicSearchParameter().getSwitchList() != null
                    && searchHotelDataRequest.getPublicSearchParameter().getSwitchList().contains(100005)) {
                indexedTags.put("prefetchdata", "3");
            }

            if (SearchServiceConfig.getInstance().getEnableRecordRawLocale()){
                indexedTags.put("locale",  CServiceUtil.getLocaleStringNoDefault());
            }else {
                indexedTags.put("locale", responseInfo.getLocale());
            }



            indexedTags.put("ibugroup", responseInfo.getGroup());
            indexedTags.put("platform", responseInfo.getPlatform());
            if (SearchServiceConfig.getInstance().getEnableCountryCode()) {
                indexedTags.put("countrycode", responseInfo.getCountryCode());
            }
        }


        if (!SSContext.getBusinessConfig().getEnableHotelListRecordErrorcode()) {
            if (!StringCommonUtils.isNullOrEmpty(errorCode)) {
                indexedTags.put("errorcode", errorCode);
            }
        }


        if (!StringCommonUtils.isNullOrEmpty(notCanReserveCode)) {
            indexedTags.put("notcanreservecode", notCanReserveCode);
        }

        // ??????ID??????
        if (args != null) {
            indexedTags.put("channellistset", args.getChannelList());
        }
        // ??????
        indexedTags.put("interval", String.valueOf(elapsedMilliseconds));
        if (args != null && indexedTags.containsKey("city") && "-1".equals(indexedTags.get("city"))) {
            // ????????????????????????????????????????????????????????????PkgSearchEntity.city->args.city
            indexedTags.put("city", String.valueOf(args.getCity()));
        }


        if (SearchServiceConfig.getInstance().getEnableRecordCountryID()
                && ("-1".equalsIgnoreCase(indexedTags.get("city")) || "0".equalsIgnoreCase(indexedTags.get("city")))) {
            if (responseInfo.getCityId() >= 0) {
                indexedTags.put("city", String.valueOf(responseInfo.getCityId()));
            }
        }

        if (SearchServiceConfig.getInstance().getEnableESRecordFirstHoteCity()) {
            if (responseInfo != null) {
                if (indexedTags.containsKey("city")
                        && ("-1".equals(indexedTags.get("city")) || "0".equals(indexedTags.get("city")))) {
                    // ??????HotelList??????????????????City??????????????????CityId
                    indexedTags.put("city", String.valueOf(responseInfo.getFirstHoteCity()));
                }
            }
        }

        if (SSContext.getBusinessConfig().getEnableRecordProvinceID()) {
            if (responseInfo.getProvinceId() >= 0) {
                indexedTags.put("province", String.valueOf(responseInfo.getProvinceId()));
            }
        }


        if (args != null && !StringCommonUtils.isNullOrWhiteSpace(args.getMinPriceThreeRoomLeft())) {
            indexedTags.put("minpricethreeroomleft", args.getMinPriceThreeRoomLeft());
        } else {
            indexedTags.put("minpricethreeroomleft", "F");
        }
        if (args != null && args.getMemberTypePromotion() != null) {
            indexedTags.put("pubargsmembertypepromotion", "T");
        } else {
            indexedTags.put("pubargsmembertypepromotion", "F");
        }
        if (args != null && args.getSearchPromote() != null && args.getSearchPromote().getMemberTypePromotion() != null) {
            indexedTags.put("searchpromotemembertypepromotion", "T");
        } else {
            indexedTags.put("searchpromotemembertypepromotion", "F");
        }
        if (args != null && args.getSearchPromote() != null && !StringCommonUtils.isNullOrWhiteSpace(args.getSearchPromote().getSearchTicketGiftType())) {
            indexedTags.put("searchpromoteticketgifttype", "T");
        } else {
            indexedTags.put("searchpromoteticketgifttype", "F");
        }
        // ????????????????????????, args????????????????????????????????????????????????????????????????????????
        indexedTags.put("publicparmargs", args != null && args.getGetParticularGradeRank() ? "T" : "F");
        // ???????????????????????????
        indexedTags.put("hitsearchlogic", StringCommonUtils.isNullOrWhiteSpace(hitLogicIds) ? "0" : hitLogicIds);
        // ???????????????CC_SPD????????????????????????
        if (SearchServiceConfig.getInstance().getEnableMatchSpiderLazyLogic()) {
            // ???????????????
            if (searchHotelDataRequest.getPersonaInfoEntity() != null && searchHotelDataRequest.getPersonaInfoEntity().getIsSpider()) {
                indexedTags.put("spider", "1");
            }
            // CC_SPD?????? ??????????????????
            if (SSContext.getSpiderType() > 0) {
                String spidertype = SSContext.getSpiderType().toString();
                indexedTags.put("spider", spidertype);
                String cliendAppId = "123456";
                String cliendIP = "123456";
                if (indexedTags.containsKey("clientid")) {
                    cliendAppId = indexedTags.get("clientid");
                }
                if (indexedTags.containsKey("clientip")) {
                    cliendIP = indexedTags.get("clientip");
                }
                lazyModeUtil.spiderLog(cliendAppId, cliendIP, spidertype);
                if (SearchServiceConfig.getInstance().getEnabledComputeRoomCountLimitRequestXML() && SSContext.getSpiderType() == 3) {
                    LogHelper.getInstance()
                            .logInfo("ComputeRoomCountLimit", JsonUtil.stringify(searchHotelDataRequest)
                                    , GettingRequestHelper.getAdditionalInfo(searchHotelDataRequest, searchHotelDataRequest.getSearchTypeEntity().getSearchType().toString(), cliendAppId, shardIndex));
                }
            }
        }
        if (searchHotelDataRequest.getPersonaInfoEntity() != null) {
            indexedTags.put("deviceno", searchHotelDataRequest.getPersonaInfoEntity().getDeviceNo());
        }
        HashMap<String, String> storedTags = new HashMap<>(20);
        cRequest = null;
        if (SearchServiceConfig.getInstance().getIsEnableLogToCatWithStoredTags()
                && HttpRequestContext.getInstance() != null
                && HttpRequestContext.getInstance().request() != null) {
            cRequest = HttpRequestContext.getInstance().request();
        }
        if (responseInfo != null) {
            indexedTags.put("lazystatisticsroomcount", String.valueOf(responseInfo.getLazyStatisticsRoomCount()));
            storedTags.put("floatingprice", responseInfo.getFloatingPriceSB());
            storedTags.put("hotellastroomfilterreason", responseInfo.getHotelLastRoomFilterReason());
            storedTags.put(CustomerTagKeyConst.FilterPrepayDiscountTagIDList, responseInfo.getFilterPrepayDiscountTagIDList());
            if (responseInfo.getHotelMinPriceBottomSet() != null && responseInfo.getHotelMinPriceBottomSet().size() > 0) {
                storedTags.put("hotelminpricebottomset", StringCommonUtils.join(responseInfo.getHotelMinPriceBottomSet(), ","));
                indexedTags.put("hotelminpricebottomcount", String.valueOf(responseInfo.getHotelMinPriceBottomSet().size()));
            } else {
                indexedTags.put("hotelminpricebottomcount", "0");
            }
            if (responseInfo.getRoomIdList() != null && responseInfo.getRoomIdList().size() > 0) {
                storedTags.put("roomIds", StringCommonUtils.join(responseInfo.getRoomIdList(), ","));
            }
            if (!StringCommonUtils.isNullOrEmpty(responseInfo.getInterfaceLimitHotelIdMsg())) {
                storedTags.put("interfacelimithotelidmsg", responseInfo.getInterfaceLimitHotelIdMsg());
            }
            if (SSContext.getBusinessConfig().getEnableRecordNotBookHotelCount()) {
                indexedTags.put("notBookHotelBottomCount", String.valueOf(responseInfo.getNotBookHotelbottomCount()));
                indexedTags.put("notBookHotelBottomPercent", String.format( "%.2f",responseInfo.getNotBookHotelbottomPercent()));
            }
        }
        if (cRequest != null) {
            if (SSContext.isRatePlanRQ()) {
                indexedTags.put("ss-client-id", StringUtils.defaultIfBlank(cRequest.clientAppId(), ""));
            } else {
                indexedTags.put("ss-client-id", StringUtils.defaultIfBlank(cRequest.getHeader("ss-client-id"), ""));
            }
            if (SearchServiceConfig.getInstance().getEnableLogProductTestTag()) {
                indexedTags.put("traffictag", StringUtils.defaultIfBlank(cRequest.getHeader("x-ctx-pressure-lane"), ""));
                indexedTags.put("traffictag", StringUtils.defaultIfBlank(cRequest.getHeader("ctx-pro-test-lane"), ""));
                setTrafficTag(indexedTags,cRequest);
            }
        }
        if (SearchServiceConfig.getInstance().getEanbleUseRequestSsClientAppId()
                && !StringCommonUtils.isNullOrWhiteSpace(searchHotelDataRequest.getPublicSearchParameter().getSsClientAppId())
                && !SearchServiceConfig.getInstance().getEnableSearchServiceEsAppidSwitch()) {
            if (SSContext.isRatePlanRQ()) {
                if (cRequest != null) {
                    indexedTags.put("ss-client-id", cRequest.clientAppId());
                }
            } else {
                indexedTags.put("ss-client-id", searchHotelDataRequest.getPublicSearchParameter().getSsClientAppId());
            }
        }
        if (SearchServiceConfig.getInstance().getEnableESRecordGroupId()) {
            indexedTags.put("groupid", Foundation.group().getId());
        }
        if (SearchServiceConfig.getInstance().getIsEnableLogToCatWithStoredTags()
                && cRequest != null) {
            if (SearchServiceConfig.getInstance().getIsEnableLogToCatWithAllHeaders()) {
                for (String key : cRequest.requestHeaderNames()) {
                    storedTags.put(key, cRequest.getHeader(key));
                }
            }
            // Clog????????????ID
            String clogTraceID = "";
            if (cRequest != null) {
                clogTraceID = (cRequest.getHeader("CLOGGING_TRACE_ID") != null) ? cRequest.getHeader("CLOGGING_TRACE_ID") : cRequest.getHeader("Tracing-TraceId");
            }
            storedTags.put("clog-trace-id", (clogTraceID != null) ? clogTraceID : "");
            // Cat????????????ID
            if (catProxyCheckServiceCall(cRequest)) {
                MessageTree catTree = Cat.getManager().getThreadLocalMessageTree();
                if (catTree != null) {
                    storedTags.put("cat-root-id", catTree.getRootMessageId());
                    storedTags.put("cat-parent-id", catTree.getParentMessageId());
                    storedTags.put("messageid", catTree.getMessageId());
                }
            }
            if (responseInfo != null) {
                indexedTags.put("rotaryhotelcount", String.valueOf(responseInfo.getRotaryHotelCount()));
                indexedTags.put("rotaryroomcount", String.valueOf(responseInfo.getRotaryRoomCount()));
                indexedTags.put("rotarytotaltime", String.valueOf(responseInfo.getRotaryTotalTime()));
                storedTags.put("request", responseInfo.getRequestForLargeRotaryTotalTime());
                if (responseInfo.getResponseAllHotelCount() >= 0) {
                    storedTags.put("custom_redirect", responseInfo.getRedirectMsg());
                    if (SearchServiceConfig.getInstance().getAddESRoomStatusWithRequestList()) {
                        // ????????????????????????
                        storedTags.put("allroomstatus", StringUtils.trimToEmpty(responseInfo.getAllRoomStatus()));
                        storedTags.put("allroomstatus2", StringUtils.trimToEmpty(responseInfo.getAllRoomStatus2()));
                    } else {
                        if (responseInfo.getResponseAllHotelCount() == 1) {
                            // ????????????????????????
                            storedTags.put("allroomstatus", StringUtils.trimToEmpty(responseInfo.getAllRoomStatus()));
                            storedTags.put("allroomstatus2", StringUtils.trimToEmpty(responseInfo.getAllRoomStatus2()));
                        }
                    }
                    if (responseInfo.getResponseAllHotelCount() == 1) {
                        // ????????????????????????
                        storedTags.put("allroomstatus", StringUtils.trimToEmpty(responseInfo.getAllRoomStatus()));
                    }
                }
                if (!StringCommonUtils.isNullOrWhiteSpace(responseInfo.getAllHotelStatus())) {
                    storedTags.put("allhotelstatus", responseInfo.getAllHotelStatus());
                }
                if (responseInfo.getLoggerRoomPriceMessage() != null) {
                    int len = Math.min(loggerRoomPriceMessageKeys.length, responseInfo.getLoggerRoomPriceMessage().size());
                    for (int i = 0; i < len; i++) {
                        storedTags.put(loggerRoomPriceMessageKeys[i], responseInfo.getLoggerRoomPriceMessage().get(i));
                    }
                }
                if (!StringCommonUtils.isNullOrEmpty(responseInfo.getFreeRoomDebugInfo())) {
                    storedTags.put("freeroomdebuginfo", responseInfo.getFreeRoomDebugInfo());
                }
            }
            if (args != null) {
                if (!StringCommonUtils.isNullOrEmpty(args.getKeyWord())) {
                    storedTags.put("keywordtext", args.getKeyWord());
                }
                if (!StringCommonUtils.isNullOrEmpty(args.getHotelName())) {
                    storedTags.put("hotelnametext", args.getHotelName());
                }
                if (!StringCommonUtils.isNullOrEmpty(args.getFilterTicketPlatformPromotionIDList())) {
                    storedTags.put("usercouponlist", args.getFilterTicketPlatformPromotionIDList());
                }
                if (!StringCommonUtils.isNullOrEmpty(args.getPlatformPromotionIDList())) {
                    storedTags.put("filtercouponlist", args.getPlatformPromotionIDList());
                }
            }
        }

        if (SearchServiceConfig.getInstance().getEnableLogErrorFlagToES()) {
            // ?????????????????????????????????????????????
            storedTags.put("haserror", (SSContext.getTraceInfo() != null && SSContext.getTraceInfo().getCatchedExceptions() > 0) ? "T" : "F");
        }
        if (SearchServiceConfig.getInstance().getLogCatBlockedKeyNames() != null && CollectionUtils.isNotEmpty(SearchServiceConfig.getInstance().getLogCatBlockedKeyNames())) {
            for (String key : SearchServiceConfig.getInstance().getLogCatBlockedKeyNames()) {
                if (indexedTags.containsKey(key)) {
                    indexedTags.remove(key);
                }
                if (storedTags.containsKey(key)) {
                    storedTags.remove(key);
                }
            }
        }
        if (SearchServiceConfig.getInstance().getEnableLogRequestToCat()) {
            String requestText = SSContext.getRequestText();
            String requestType = SSContext.getRequestType();
            if (StringCommonUtils.isNullOrWhiteSpace(requestText)) {
                storedTags.put("rqtype", StringUtils.trimToEmpty(requestType));
                storedTags.put("rqtext", StringUtils.trimToEmpty(requestText));
            }
        }
        if (SSContext.getTraceInfo() != null) {
            storedTags.put("computehotelcount", String.valueOf(SSContext.getTraceInfo().getComputeHotelCount()));
            storedTags.put("calledapicount", String.valueOf(SSContext.getTraceInfo().getCalledApiCount()));
        }
        if (SearchServiceConfig.getInstance().getIsMultiDayNewLogicV2UseMultiDayStatus()) {
            if (SSContext.getTraceInfo() != null) {
                indexedTags.put("roominfocopycount", String.valueOf(SSContext.getTraceInfo().getRoomInfoCopyTimes()));
            } else {
                indexedTags.put("roominfocopycount", "0");
            }
        }

        if (debugEntity != null) {
            indexedTags.put("loadmasterhoteltime", String.valueOf((debugEntity.getLoadHotelCost_LoadMaster() * 1000)));
            indexedTags.put("filtermasterhoteltime", String.valueOf((debugEntity.getLoadHotelCost_PreStaticFilter() * 1000)));
            indexedTags.put("filtersubhoteltime", String.valueOf((debugEntity.getLoadHotelCost_ProcessMaster() * 1000)));

            indexedTags.put("hotelsorttime", String.valueOf((debugEntity.getSortHotelTime() * 1000)));
            indexedTags.put("heapsorttime", String.valueOf((debugEntity.getHeapSortTime() * 1000)));
            indexedTags.put("gethoteltime", String.valueOf((debugEntity.getGetCacheData() * 1000)));
            indexedTags.put("af_h_filter", String.valueOf(debugEntity.getWowcomputehotelcount()));
            indexedTags.put("af_r_filter", String.valueOf(debugEntity.getWowcomputeroomcount()));
            indexedTags.put("bf_h_filter", String.valueOf(debugEntity.getWowhotelcount()));

            if (SSContext.getBusinessConfig().getEnableHotelListRecordErrorcode()){
                if (debugEntity.isSingleHotelMark()) {
                    indexedTags.put("hotelname","singlehotel");
                    if (!com.ctrip.hotel.product.search.cache.common.util.StringCommonUtils.isNullOrWhiteSpace(errorCode)
                            && debugEntity.getWowcomputeroomcount() == 0) {
                        indexedTags.put("errorcode", errorCode);
                    }
                }
            }

            if (SearchServiceConfig.getInstance().getFixCKInfo4SearchHotelCount()){
                indexedTags.put("wowhotelcount", String.valueOf(debugEntity.getSearchHCTotalCount()));
            }
            indexedTags.put("computesubhotelcount", String.valueOf(debugEntity.getComputeSubHotelCount()));
            indexedTags.put("lazyloadroomcosttime", String.valueOf(debugEntity.getLazyLoadRoomCost() * 1000));
            indexedTags.put("dystep1", String.valueOf(debugEntity.getFilterTime() * 1000));
            indexedTags.put("dystep2", String.valueOf(debugEntity.getDynamicProcess_ProcessExCost() * 1000));
            indexedTags.put("bdbstatus", String.valueOf(debugEntity.getBDBStatus()));
            indexedTags.put("dbsoastatus", String.valueOf(debugEntity.getDBSOAStatus()));

            indexedTags.put("interval_rotatereform", String.valueOf(debugEntity.getInterval_rotatereform()));

            indexedTags.put("totalroomcount", String.valueOf(debugEntity.getTotalRoomCount()));
            indexedTags.put("outputroomcount", String.valueOf(debugEntity.getOutputRoomCount()));
            indexedTags.put("roomoutputrate", String.valueOf(debugEntity.getRoomOutputRate()));

            if (SearchServiceConfig.getInstance().getIsEnableLSLogInfo() && debugEntity.getCallSoaHotelCount() > 0) {
                indexedTags.put("callsoahotelcount", String.valueOf(debugEntity.getCallSoaHotelCount()));
                indexedTags.put("callsoacount", String.valueOf(debugEntity.getCallSoaCount()));
                indexedTags.put("lsfilterratio", String.valueOf(debugEntity.getLsFilterRatio()));
                indexedTags.put("callsoatimeout4j", String.valueOf(debugEntity.isCallSoaTimeOut4J() ? "T" : "F"));
                indexedTags.put("callsoatimeoutcount", String.valueOf(debugEntity.getCallSoaTimeOutCount()));
                indexedTags.put("callsoafinaltimeoutcount", String.valueOf(debugEntity.getCallSoaFinalTimeOutCount()));
                if (!StringCommonUtils.isNullOrEmpty(debugEntity.getCallSoaHotelList())) {
                    indexedTags.put("callsoahotellist", String.valueOf(debugEntity.getCallSoaHotelList()));
                }
                setCallSoaNotCanBook(debugEntity,indexedTags);

                if (!StringCommonUtils.isNullOrEmpty(debugEntity.getCallSoaTimeOutHotelList())) {
                    indexedTags.put("callsoatimeouthotellist", String.valueOf(debugEntity.getCallSoaTimeOutHotelList()));
                }

                if (!StringCommonUtils.isNullOrEmpty(debugEntity.getFilterItems())) {
                    indexedTags.put("filteritems", String.valueOf(debugEntity.getFilterItems()));
                }
                if (SearchServiceConfig.getInstance().getEnableLSTopAndBottomInfo()) {
                    indexedTags.put("forcetop", String.valueOf(debugEntity.getForceTopHotelList()));
                    indexedTags.put("forcebottom", String.valueOf(debugEntity.getForceBottomHotelList()));
                    indexedTags.put("rqmd5", String.valueOf(debugEntity.getRqMd5()));

                    indexedTags.put("outputhotels", String.valueOf(debugEntity.getOutputHotels()));
                }
            }
            if (debugEntity.getUserFilterTagId() != null && debugEntity.getUserFilterTagId().size() > 0) {
                indexedTags.put("uesrtagIdList", debugEntity.getUserFilterTagId().stream().map(x -> "" + x).collect(Collectors.joining(","))); }
        } else {
            indexedTags.put("hotelname", "DebugEntityNull");
        }
        // ReturnClassList??????????????????
        if (searchHotelDataRequest.getReturnClassList() != null) {
            indexedTags.put("hotelsosomap", searchHotelDataRequest.getReturnClassList().contains(ReturnClass.HotelSosoMapInfoEntity) ? "T" : "F");
            // ??????????????????????????????
            // ???????????????????????????????????????
            if (searchHotelDataRequest.getReturnClassList() != null &&
                    CollectionUtils.containsAny(searchHotelDataRequest.getReturnClassList(), new ArrayList() {{
                        add(ReturnClass.BaseRoomFacilityEntitys);
                        add(ReturnClass.FacilityAndHotelEntity);
                        add(ReturnClass.HotelFacilitiesEntity);
                    }})) {
                indexedTags.put("ressize", "1");
            }

        }
        if (searchHotelDataRequest.getReturnClassList() != null) {
            indexedTags.put("isgetallrooms", searchHotelDataRequest.getReturnClassList().contains(ReturnClass.BaseRoomEntity) ? "T" : "F");
        }
        if (debugEntity != null && debugEntity.getInterfaceRedisInfo() != null) {
            InterfaceRedisMetric info = debugEntity.getInterfaceRedisInfo();
            indexedTags.put("interfaceredis_totaltime", String.valueOf(info.getTotalTime()));
            indexedTags.put("interfaceredis_readtimes", String.valueOf(info.getReadTimes()));
            indexedTags.put("interfaceredis_readredistime", String.valueOf(info.getReadRedisiTime()));
            indexedTags.put("interfaceredis_callpricingtime", String.valueOf(info.getCallPricingTime()));
            indexedTags.put("interfaceredis_callriskctrltime", String.valueOf(info.getCallRiskCtrlTime()));
            indexedTags.put("interfaceredis_mergetime", String.valueOf(info.getMergeTime()));
        }
        ConcurrentMap<String, TimeCostEntity> timeCostDic = ContextKeyUtils.getContextItem(ContextItemKey.TimeCostDic, ConcurrentMap.class);
        if (timeCostDic != null && timeCostDic.size() > 0) {
            Map<String, String> hotelShardIndexTags = new HashMap<String, String>();
            Map<String, String> hotelShardStoreTags = new HashMap<String, String>();
            hotelShardIndexTags.put("tracelogid", searchHotelDataRequest.getPublicSearchParameter().getLogId() != null ? searchHotelDataRequest.getPublicSearchParameter().getLogId() : "");
            hotelShardIndexTags.put("interval", String.valueOf(elapsedMilliseconds));
            if (debugEntity != null) {
                hotelShardIndexTags.put("loadmasterhotel", String.valueOf((debugEntity.getLoadHotelCost_LoadMaster() * 1000)));
                hotelShardIndexTags.put("prestaticfilter", String.valueOf((debugEntity.getLoadHotelCost_PreStaticFilter() * 1000)));
                hotelShardIndexTags.put("findsubhotel", String.valueOf((debugEntity.getLHC_PM_FilterSubHotel() * 1000)));
            }
            long redisCacheTimeCost = 0;
            for (Map.Entry<String, TimeCostEntity> kv : timeCostDic.entrySet()) {
                if (kv.getValue().isIndexTag()) {
                    String indexStr = "interval_" + kv.getKey().toLowerCase();
                    hotelShardIndexTags.put(indexStr, String.valueOf(kv.getValue().getMaxCost()));
                }
                if (kv.getKey().startsWith(TimeCostConst.KeyCache)) {
                    redisCacheTimeCost += kv.getValue().getMaxCost();
                }
            }
            hotelShardIndexTags.put("interval_rediscache", String.valueOf(redisCacheTimeCost));
            hotelShardStoreTags.put("interval_detail", JsonUtil.stringify(timeCostDic.entrySet().stream().filter(p -> p.getValue() != null && p.getValue().getTotalCost() > 0)));
            Cat.logTags("hotel-shard-timecost", hotelShardIndexTags, hotelShardStoreTags);
        }

        double interval_redis_total = 0;
        double interval_redis_deserialize_total = 0;
        long redis_size_total = 0;
        int redis_count_total = 0;
        double interval_setdata_total = 0;
        for (Map.Entry<String, String> entry : SSContext.getCatIndexTags().entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }

            if (entry.getKey().startsWith("interval_redis_")) {
                double interval_redis_deserialize = NumberConverter.convertToDouble(entry.getValue(), 0);
                if (entry.getKey().endsWith("_deserialize")) {
                    interval_redis_deserialize_total += interval_redis_deserialize;
                } else {
                    interval_redis_total += interval_redis_deserialize;
                }
                continue;
            }

            if (entry.getKey().startsWith("redis_")) {
                if (entry.getKey().endsWith("_count")) {
                    redis_count_total += NumberConverter.parseInt(entry.getValue(), 0);
                } else if (entry.getKey().endsWith("_size")) {
                    redis_size_total += com.ctrip.hotel.product.search.cache.common.util.StringCommonUtils.parseLong(entry.getValue(), 0);
                }
            }

            if (entry.getKey().startsWith("interval_setdata_")) {
                double interval_setdata_time = NumberConverter.convertToDouble(entry.getValue(), 0);
                interval_setdata_total += interval_setdata_time;
            }
        }
        SSContext.getCatIndexTags().put("interval_redis_total", String.valueOf(interval_redis_total));
        SSContext.getCatIndexTags().put("interval_redis_deserialize_total", String.valueOf(interval_redis_deserialize_total));
        SSContext.getCatIndexTags().put("redis_count_total", String.valueOf(redis_count_total));
        SSContext.getCatIndexTags().put("redis_size_total", String.valueOf(redis_size_total));
        SSContext.getCatIndexTags().put("interval_setdata_total", String.valueOf(interval_setdata_total));

        if (SearchServiceConfig.getInstance().getUseTaskBitFlag()) {
            TaskBitFlagWriter.writeToES(indexedTags);
        }

        if (SearchServiceConfig.getInstance().getRecordTimeCostForRedis()) {
            // Map<String, VmsReadRedisTimeCostEntity> vmsReadRedisTimeCostMap = ContextKeyUtils.getContextItem(ContextItemKey.VMSReadRedisTimeCostDic);
            Map<String, VmsReadRedisTimeCostEntity> vmsReadRedisTimeCostMap = SSContext.getReadRedisTimeCost();
            if (vmsReadRedisTimeCostMap != null && vmsReadRedisTimeCostMap.size() > 0) {
                double total = 0;
                indexedTags.put("interval_redis_roomabase", "0.0");
                indexedTags.put("interval_redis_roombase_size", "0");
                indexedTags.put("interval_redis_roombase_roomcount", "0");
                indexedTags.put("roombase_count", "0");
                indexedTags.put("interval_redis_hotelbase", "0.0");
                indexedTags.put("interval_redis_hotelbase_size", "0");
                indexedTags.put("hotelbase_count", "0");
                indexedTags.put("interval_redis_hotelextra", "0.0");
                indexedTags.put("interval_redis_hotelextra_size", "0");
                indexedTags.put("hotelextra_count", "0");
                indexedTags.put("interval_redis_singleroomprice", "0.0");
                indexedTags.put("interval_redis_singleroomprice_readcount", "0");
                indexedTags.put("interval_redis_multiroomprice", "0.0");
                indexedTags.put("interval_redis_multiroomprice_readcount", "0");
                indexedTags.put("interval_redis_otherkeycache", "0.0");
                indexedTags.put("interval_desc_roombase", "0.0");
                indexedTags.put("interval_desc_hotelbase", "0.0");
                indexedTags.put("interval_desc_hotelextra", "0.0");
                indexedTags.put("interval_desc_singleroomprice", "0.0");
                indexedTags.put("interval_desc_multiroomprice", "0.0");
                indexedTags.put("interval_desc_otherkeycache", "0.0");

                for (Map.Entry<String, VmsReadRedisTimeCostEntity> kv : vmsReadRedisTimeCostMap.entrySet()) {
                    if (kv.getValue().isIndexTag()) {
                        total = total + kv.getValue().getTotalCost();
                        indexedTags.put(kv.getKey(), String.valueOf(kv.getValue().getTotalCost() * 1000));
                        if (StringCommonUtils.isEquals("interval_redis_roomabase", kv.getKey())) {
                            indexedTags.put("interval_redis_roombase_size", String.valueOf(kv.getValue().getMaxSize()));
                            // ??????????????? ?????????redis?????????????????????
                            indexedTags.put("interval_redis_roomabase_max", String.valueOf(kv.getValue().getMaxCost() * 1000));
                            if (kv.getValue().getCount() > 0) {
                                indexedTags.put("interval_redis_roomabase_avg", String.valueOf(kv.getValue().getTotalCost() / kv.getValue().getCount() * 1000));
                            }
                            indexedTags.put("roombase_count", String.valueOf(kv.getValue().getCount()));
                        }

                        if (StringCommonUtils.isEquals("interval_desc_roombase", kv.getKey())) {
                            indexedTags.put("interval_redis_roombase_roomcount", String.valueOf(kv.getValue().getRoomCount()));
                            // ??????????????? ???????????????????????????????????????
                            indexedTags.put("interval_desc_roombase_max", String.valueOf(kv.getValue().getMaxCost() * 1000));
                            if (kv.getValue().getCount() > 0) {
                                indexedTags.put("interval_desc_roombase_avg", String.valueOf(kv.getValue().getTotalCost() / kv.getValue().getCount() * 1000));
                            }
                        }

                        if (StringCommonUtils.isEquals("interval_redis_hotelbase", kv.getKey())) {
                            indexedTags.put("interval_redis_hotelbase_size", String.valueOf(kv.getValue().getMaxSize()));
                            indexedTags.put("hotelbase_count", String.valueOf(kv.getValue().getCount()));
                        }

                        if (StringCommonUtils.isEquals("interval_redis_hotelextra", kv.getKey())) {
                            indexedTags.put("interval_redis_hotelextra_size", String.valueOf(kv.getValue().getMaxSize()));
                            indexedTags.put("hotelextra_count", String.valueOf(kv.getValue().getCount()));
                        }

                        if (StringCommonUtils.isEquals("interval_redis_singleroomprice", kv.getKey())) {
                            indexedTags.put("interval_redis_singleroomprice_readcount", String.valueOf(kv.getValue().getCount()));
                        }

                        if (StringCommonUtils.isEquals("interval_redis_multiroomprice", kv.getKey())) {
                            indexedTags.put("interval_redis_singleroomprice_readcount", String.valueOf(kv.getValue().getCount()));
                        }
                    }
                }
                indexedTags.put("interval_redis_desc_total", String.valueOf(total * 1000));
            }
        }

        if (SearchServiceConfig.getInstance().getRecordEsUpStreamAppID()) {
            String upstreamAppID = responseInfo != null ? responseInfo.getUpStreamAppID() : "-1";
            indexedTags.put("upstreamappid", upstreamAppID);
        }
        if (SearchServiceConfig.getInstance().getEsRecordServiceType()) {
            indexedTags.put("servicetype", responseInfo != null ? responseInfo.getServiceType() : "1");
        }
        if (SearchServiceConfig.getInstance().getEnableDebugEntityLocalDateTime()) {
            indexedTags.put("checkinoffset", responseInfo != null ? String.valueOf(responseInfo.getCheckinoffset()) : "-99");
        }

        if (SSContext.getBusinessConfig().getEnableRecordCheckinOffsetDaysToInt()) {
            indexedTags.put("checkin_offset_days", responseInfo != null ? String.valueOf(responseInfo.getCheckinoffset()) : "-99");
        }


        if (SearchServiceConfig.getInstance().getRecordFilterRoomSupplierRemoveRooms()) {
            if (SearchServiceConfig.getInstance().getRecordFilterRoomSupplierRemoveRoomInfos()) {
                indexedTags.put("filterroomsupplier", responseInfo != null && responseInfo.getFilterRoomSupplierAllRemoveRoomIdList() != null
                        ? StringCommonUtils.join(responseInfo.getFilterRoomSupplierAllRemoveRoomIdList()) : "");
            }
            indexedTags.put("filterroomsuppliercount", responseInfo != null && responseInfo.getFilterRoomSupplierAllRemoveRoomIdList() != null
                    ? String.valueOf(responseInfo.getFilterRoomSupplierAllRemoveRoomIdList().size()) : "0");
            indexedTags.put("filterroomsuppliernum", responseInfo != null && responseInfo.getFilterRoomSupplierAllRemoveRoomIdList() != null
                    ? String.valueOf(responseInfo.getFilterRoomSupplierAllRemoveRoomIdList().size()) : "0");
        }
        if (SearchServiceConfig.getInstance().getEnableHotelListkClickHouseSplit()) {
            updateSSContextCatTagsV2(indexedTags, searchHotelDataRequest,debugEntity,responseInfo);
        }
        removeCKField(indexedTags);
        updateSSContextCatTags(indexedTags, storedTags);
    }
    private void removeCKField(Map<String, String> indexedTags) {
        if (SearchServiceConfig.getInstance().getEnableRemoveHotelSearchserviceckRecord()) {
            indexedTags.put("pageindex","");
            indexedTags.put("ordername","");
            indexedTags.put("ordertype","");
            indexedTags.put("hotellist","");
            indexedTags.put("dotx","");
            indexedTags.put("dotx2","");
            indexedTags.put("doty","");
            indexedTags.put("doty2","");
            indexedTags.put("keyword","");
            indexedTags.put("maxlat","");
            indexedTags.put("minlat","");
            indexedTags.put("maxlng","");
            indexedTags.put("minlng","");
            indexedTags.put("radius","");
            indexedTags.put("star","");
            indexedTags.put("starlist","");
            indexedTags.put("hotelbrandlist","");
            indexedTags.put("propertycodes","");
            indexedTags.put("breakfast","");
            indexedTags.put("propertyvalueidlist","");
            indexedTags.put("pricearea","");
        }
    }
    private void logToCat(HotelCalendarSearchRequest hotelCalendarSearchRequest, long elapsedMilliseconds, CatLogStruct responseInfo, DebugEntity debugEntity, String hitLogicIds,
                          String cacheMode, String errorCode, String notCanReserveCode, int shardIndex) {
        if (!SearchServiceConfig.getInstance().getLogCat()) {
            return;
        }
        HttpRequestWrapper cRequest = null;
        if (HttpRequestContext.getInstance() != null) {
            cRequest = HttpRequestContext.getInstance().request();
        }
        if (cRequest == null || hotelCalendarSearchRequest == null || hotelCalendarSearchRequest.getPublicSearchParameter() == null || FacadeCommonUtils.getCatLogFields(cRequest) == null) {
            return;
        }
        PublicParametersEntity args = hotelCalendarSearchRequest.getPublicSearchParameter();
        Map<String, String> indexedTags = FacadeCommonUtils.getCatLogFields(cRequest);
        /**
         * ??????!!!????????????????????????indexedTags.Add??? ????????????????????????ExtendFunction.cs/CollectFieldsForCat????????????,?????????Dictionary????????????????????????????????????!!! by*
         * jwyuan@ctrip.com
         */
        if (responseInfo != null) {
            if (responseInfo.getResponseAllHotelCount() >= 0) {
                // ?????????????????????????????????????????????
                indexedTags.put("rs_hotels", String.valueOf(responseInfo.getResponseHotelCount()));
                if (hotelCalendarSearchRequest.getSearchTypeEntity().getHotelCount() > 0) {
                    indexedTags.put("fillrate", String.valueOf(CommonUtils.round2FractionalDigits(((double) responseInfo.getResponseHotelCount() / (double) hotelCalendarSearchRequest.getSearchTypeEntity().getHotelCount()), 2)));
                }
                // ???????????????????????????????????????
                indexedTags.put("responseallhotelcount", String.valueOf(responseInfo.getResponseAllHotelCount()));
                // ???????????????
                indexedTags.put("minprice", String.valueOf(responseInfo.getMinPrice()));
                // ?????????????????????
                indexedTags.put("minpriceroom", String.valueOf(responseInfo.getMinPriceRoom()));
                // ????????????????????????
                indexedTags.put("vendorid", String.valueOf(responseInfo.getVendorId()));
                // ???????????????????????????????????????????????????
                indexedTags.put("computeroomnum", String.valueOf(responseInfo.getComputeRoomCount()));
                // ?????????????????????????????????????????????????????????????????????
                indexedTags.put("computestoproomnum", String.valueOf(responseInfo.getComputeStopRoomCount()));
            }

            // ?????????????????????Country
            if (SearchServiceConfig.getInstance().getEnableRecordCountryID()
                    && responseInfo.getCountryId() > 0) {
                indexedTags.put("country", String.valueOf(responseInfo.getCountryId()));

            } else {
                // ?????????????????????Country
                indexedTags.put("country", String.valueOf(responseInfo.getCountryId()));
            }


            // ??????????????????iscanreserve
            indexedTags.put("requestlocal", (responseInfo.getOnlyRequestLocal() ? "T" : "F"));
            indexedTags.put("ticketgiftsnum", String.valueOf(responseInfo.getTicketGiftsNum()));
            indexedTags.put("optionalhotelnum", String.valueOf(responseInfo.getOptionalHotelNum()));
            indexedTags.put("floatingpricenum", String.valueOf(responseInfo.getFloatingPriceNum()));
            indexedTags.put("ticketusercouponnum", String.valueOf(responseInfo.getTicketUserCouponNum()));
            indexedTags.put("resetroomdinfonum", String.valueOf(responseInfo.getResetRoomDInfoCount()));
            indexedTags.put("shadowroomfiltertype", String.valueOf(responseInfo.getShadowRoomFilterType()));
            indexedTags.put("shadowdiscountfilter", String.valueOf(responseInfo.getShadowDiscountFilterType()));
            indexedTags.put("meta_tolerance", String.valueOf(responseInfo.getHasMetaTolerance() ? 1 : 0));
            indexedTags.put("tolerance_diffprice", String.valueOf(responseInfo.getToleranceDiffPrice()));
            indexedTags.put("tolerance_reason", String.valueOf(responseInfo.getShadowToleranceReason()));
            indexedTags.put("competion_info", String.valueOf(responseInfo.getCompetitionInfo()));
            indexedTags.put("shadowroomcount", String.valueOf(responseInfo.getShadowRoomCount()));
            indexedTags.put("personpriceroomnum", String.valueOf(responseInfo.getMultiPersonPriceRoomNum()));
            indexedTags.put("hotelavailrs", responseInfo.getHotelAvailSubHotelList());
            indexedTags.put("hotelavailsuccesscount", String.valueOf(responseInfo.getHotelAvailSuccessCount()));
            indexedTags.put("hotelavailtimeoutcount", String.valueOf(responseInfo.getHotelAvailTimeoutCount()));
            indexedTags.put("hotelavailtotalcount", String.valueOf(responseInfo.getHotelAvailTotalCount()));
            indexedTags.put("directbatchstatus", responseInfo.getDirectBatchStatus());
            indexedTags.put("interval_htlavail", String.valueOf(responseInfo.getInterval_htlavail()));
            indexedTags.put("interval_shadow", String.valueOf(responseInfo.getInterval_shadow()));
            indexedTags.put("interval_ibufilter", String.valueOf(responseInfo.getInterval_ibufilter()));
            indexedTags.put("interval_makeprice", String.valueOf(responseInfo.getInterval_makeprice()));
            indexedTags.put("makeprice_detail", responseInfo.getMakeprice_detail());
            indexedTags.put("interval_realtime", String.valueOf(responseInfo.getInterval_realtime()));
            indexedTags.put("interval_realtimepricing", String.valueOf(responseInfo.getInterval_realtimepricing()));
            indexedTags.put("interval_realtimepricing_total", String.valueOf(responseInfo.getInterval_realtimepricing_total()));
            indexedTags.put("count_realtimepricing", String.valueOf(responseInfo.getCount_realtimepricing()));
            indexedTags.put("filter_realtime", responseInfo.getFilter_realtime());
            indexedTags.put("interval_pricingprofit", String.valueOf(responseInfo.getInterval_pricingprofit()));
            indexedTags.put("interval_pricechangeblack", String.valueOf(responseInfo.getInterval_pricechangeblack()));
            indexedTags.put("isovsshadowallroomsoa", responseInfo.getHasOvsAllRoomSOA() ? "T" : "F");
            indexedTags.put("calcbestcoupon", args.getIsCalcRoomTicketAmountForUserCoupon() ? "T" : "F");
            indexedTags.put("hotelshadows", responseInfo.getShadow_RoomDetailList());
            indexedTags.put("hotelshadowminprices", responseInfo.getShadow_MinPriceRooms());
            indexedTags.put("allmarklogic", responseInfo.getAllMarkLogic());
            indexedTags.put("hotelshadowmisshotels", responseInfo.getShadow_MissHotels());
            indexedTags.put("shadowavailtime", String.valueOf(responseInfo.getShadowAvailTime()));
            indexedTags.put("roomsinglestr", responseInfo.getRoomSingleStr());
            indexedTags.put("clearandmappingtime", String.valueOf(responseInfo.getClearAndMappingTime()));
            indexedTags.put("hotelminpriceresetzerocount", String.valueOf(responseInfo.getHotelMinPriceResetZeroCount()));
            // recommend
            indexedTags.put("recommendcomputeroomcount", String.valueOf(responseInfo.getRecommendComputeRoomCount()));
            indexedTags.put("recommendcomputehotelcount", String.valueOf(responseInfo.getRecommendComputeHotelCount()));
            indexedTags.put("recommendhotelcount", String.valueOf(responseInfo.getRecommendHotelCount()));
            // ?????????????????????
            indexedTags.put("canbookroomnum", String.valueOf(responseInfo.getCanBookRoomNum()));
            indexedTags.put("notcanbookroomnum", String.valueOf(responseInfo.getNotCanBookRoomNum()));
            indexedTags.put("canbookhotelnum", String.valueOf(responseInfo.getCanBookHotelNum()));
            indexedTags.put("notcanbookhotelnum", String.valueOf(responseInfo.getNotCanBookHotelNum()));
            indexedTags.put("queryscence", responseInfo.getQueryScence());
            indexedTags.put(CustomerTagKeyConst.IBUPosID, responseInfo.getIbuPosId());
            indexedTags.put(CustomerTagKeyConst.CommissionRatePricingChannel, String.valueOf(responseInfo.getCommissionRatePricingChannel()));
            indexedTags.put("queryroomcount", String.valueOf((responseInfo.getRoomIdList() == null ? 0 : responseInfo.getRoomIdList().size())));
            if (responseInfo.getHotelListVisitHotelShard()) {
                indexedTags.put("prefetchdata", "1");
            } else if (hotelCalendarSearchRequest.getPublicSearchParameter() != null
                    && hotelCalendarSearchRequest.getPublicSearchParameter().getSwitchList() != null
                    && hotelCalendarSearchRequest.getPublicSearchParameter().getSwitchList().contains(100003)) {
                indexedTags.put("prefetchdata", "2");
            } else if (hotelCalendarSearchRequest.getPublicSearchParameter() != null
                    && hotelCalendarSearchRequest.getPublicSearchParameter().getSwitchList() != null
                    && hotelCalendarSearchRequest.getPublicSearchParameter().getSwitchList().contains(100005)) {
                indexedTags.put("prefetchdata", "3");
            }

            if (SearchServiceConfig.getInstance().getEnableRecordRawLocale()){
                indexedTags.put("locale",  CServiceUtil.getLocaleStringNoDefault());
            }else {
                indexedTags.put("locale", responseInfo.getLocale());
            }

            indexedTags.put("ibugroup", responseInfo.getGroup());
            indexedTags.put("platform", responseInfo.getPlatform());
            if (SearchServiceConfig.getInstance().getEnableCountryCode()) {
                indexedTags.put("countrycode", responseInfo.getCountryCode());
            }
        }

        if (!SSContext.getBusinessConfig().getEnableHotelListRecordErrorcode()) {
            if (!StringCommonUtils.isNullOrEmpty(errorCode)) {
                indexedTags.put("errorcode", errorCode);
            }
        }

        if (!StringCommonUtils.isNullOrEmpty(notCanReserveCode)) {
            indexedTags.put("notcanreservecode", notCanReserveCode);
        }
        // ????????????DB


        // ??????ID??????
        if (args != null) {
            indexedTags.put("channellistset", args.getChannelList());
        }
        // ??????
        indexedTags.put("interval", String.valueOf(elapsedMilliseconds));
        if (args != null && indexedTags.containsKey("city") && "-1".equals(indexedTags.get("city"))) {
            // ????????????????????????????????????????????????????????????PkgSearchEntity.city->args.city
            indexedTags.put("city", String.valueOf(args.getCity()));
        }


        if (SearchServiceConfig.getInstance().getEnableRecordCountryID()
                && ("-1".equalsIgnoreCase(indexedTags.get("city")) || "0".equalsIgnoreCase(indexedTags.get("city")))) {
            if (responseInfo.getCityId() >= 0) {
                indexedTags.put("city", String.valueOf(responseInfo.getCityId()));
            }
        }

        if (SearchServiceConfig.getInstance().getEnableESRecordFirstHoteCity()) {
            if (responseInfo != null) {
                if (indexedTags.containsKey("city")
                        && ("-1".equals(indexedTags.get("city")) || "0".equals(indexedTags.get("city")))) {
                    // ??????HotelList??????????????????City??????????????????CityId
                    indexedTags.put("city", String.valueOf(responseInfo.getFirstHoteCity()));
                }
            }
        }
        if (SSContext.getBusinessConfig().getEnableRecordProvinceID()) {
            if (responseInfo.getProvinceId() >= 0) {
                indexedTags.put("province", String.valueOf(responseInfo.getProvinceId()));
            }
        }

        if (args != null && !StringCommonUtils.isNullOrWhiteSpace(args.getMinPriceThreeRoomLeft())) {
            indexedTags.put("minpricethreeroomleft", args.getMinPriceThreeRoomLeft());
        } else {
            indexedTags.put("minpricethreeroomleft", "F");
        }
        if (args != null && args.getMemberTypePromotion() != null) {
            indexedTags.put("pubargsmembertypepromotion", "T");
        } else {
            indexedTags.put("pubargsmembertypepromotion", "F");
        }
        if (args != null && args.getSearchPromote() != null && args.getSearchPromote().getMemberTypePromotion() != null) {
            indexedTags.put("searchpromotemembertypepromotion", "T");
        } else {
            indexedTags.put("searchpromotemembertypepromotion", "F");
        }
        if (args != null && args.getSearchPromote() != null && !StringCommonUtils.isNullOrWhiteSpace(args.getSearchPromote().getSearchTicketGiftType())) {
            indexedTags.put("searchpromoteticketgifttype", "T");
        } else {
            indexedTags.put("searchpromoteticketgifttype", "F");
        }
        // ????????????????????????, args????????????????????????????????????????????????????????????????????????
        indexedTags.put("publicparmargs", args != null && args.getGetParticularGradeRank() ? "T" : "F");
        // ???????????????????????????
        indexedTags.put("hitsearchlogic", StringCommonUtils.isNullOrWhiteSpace(hitLogicIds) ? "0" : hitLogicIds);
        // ???????????????CC_SPD????????????????????????
        if (SearchServiceConfig.getInstance().getEnableMatchSpiderLazyLogic()) {
            // ???????????????
            if (hotelCalendarSearchRequest.getPersonaInfoEntity() != null && hotelCalendarSearchRequest.getPersonaInfoEntity().getIsSpider()) {
                indexedTags.put("spider", "1");
            }
            // CC_SPD?????? ??????????????????
            if (SSContext.getSpiderType() > 0) {
                String spidertype = SSContext.getSpiderType().toString();
                indexedTags.put("spider", spidertype);
                String cliendAppId = "123456";
                String cliendIP = "123456";
                if (indexedTags.containsKey("clientid")) {
                    cliendAppId = indexedTags.get("clientid");
                }
                if (indexedTags.containsKey("clientip")) {
                    cliendIP = indexedTags.get("clientip");
                }
                lazyModeUtil.spiderLog(cliendAppId, cliendIP, spidertype);
                if (SearchServiceConfig.getInstance().getEnabledComputeRoomCountLimitRequestXML() && SSContext.getSpiderType() == 3) {
                    LogHelper.getInstance()
                            .logInfo("ComputeRoomCountLimit", JsonUtil.stringify(hotelCalendarSearchRequest)
                                    , GettingRequestHelper.getAdditionalInfo(hotelCalendarSearchRequest, hotelCalendarSearchRequest.getSearchTypeEntity().getSearchType().toString(), cliendAppId, shardIndex));
                }
            }
        }
        if (hotelCalendarSearchRequest.getPersonaInfoEntity() != null) {
            indexedTags.put("deviceno", hotelCalendarSearchRequest.getPersonaInfoEntity().getDeviceNo());
        }
        HashMap<String, String> storedTags = new HashMap<>(20);
        cRequest = null;
        if (SearchServiceConfig.getInstance().getIsEnableLogToCatWithStoredTags()
                && HttpRequestContext.getInstance() != null
                && HttpRequestContext.getInstance().request() != null) {
            cRequest = HttpRequestContext.getInstance().request();
        }
        if (responseInfo != null) {
            indexedTags.put("lazystatisticsroomcount", String.valueOf(responseInfo.getLazyStatisticsRoomCount()));
            storedTags.put("floatingprice", responseInfo.getFloatingPriceSB());
            storedTags.put("hotellastroomfilterreason", responseInfo.getHotelLastRoomFilterReason());
            storedTags.put(CustomerTagKeyConst.FilterPrepayDiscountTagIDList, responseInfo.getFilterPrepayDiscountTagIDList());
            if (responseInfo.getHotelMinPriceBottomSet() != null && responseInfo.getHotelMinPriceBottomSet().size() > 0) {
                storedTags.put("hotelminpricebottomset", StringCommonUtils.join(responseInfo.getHotelMinPriceBottomSet(), ","));
                indexedTags.put("hotelminpricebottomcount", String.valueOf(responseInfo.getHotelMinPriceBottomSet().size()));
            } else {
                indexedTags.put("hotelminpricebottomcount", "0");
            }
            if (responseInfo.getRoomIdList() != null && responseInfo.getRoomIdList().size() > 0) {
                storedTags.put("roomIds", StringCommonUtils.join(responseInfo.getRoomIdList(), ","));
            }
            if (!StringCommonUtils.isNullOrEmpty(responseInfo.getInterfaceLimitHotelIdMsg())) {
                storedTags.put("interfacelimithotelidmsg", responseInfo.getInterfaceLimitHotelIdMsg());
            }
            if (SSContext.getBusinessConfig().getEnableRecordNotBookHotelCount()) {
                indexedTags.put("notBookHotelBottomCount", String.valueOf(responseInfo.getNotBookHotelbottomCount()));
                indexedTags.put("notBookHotelBottomPercent", String.format( "%.2f",responseInfo.getNotBookHotelbottomPercent()));
            }
        }
        if (cRequest != null) {
            if (SSContext.isRatePlanRQ()) {
                indexedTags.put("ss-client-id", StringUtils.defaultIfBlank(cRequest.clientAppId(), ""));
            } else {
                indexedTags.put("ss-client-id", StringUtils.defaultIfBlank(cRequest.getHeader("ss-client-id"), ""));
            }
            if (SearchServiceConfig.getInstance().getEnableLogProductTestTag()) {
                indexedTags.put("traffictag", StringUtils.defaultIfBlank(cRequest.getHeader("x-ctx-pressure-lane"), ""));
                indexedTags.put("traffictag", StringUtils.defaultIfBlank(cRequest.getHeader("ctx-pro-test-lane"), ""));
                setTrafficTag(indexedTags,cRequest);
            }
        }
        if (SearchServiceConfig.getInstance().getEanbleUseRequestSsClientAppId()
                && !StringCommonUtils.isNullOrWhiteSpace(hotelCalendarSearchRequest.getPublicSearchParameter().getSsClientAppId())
                && !SearchServiceConfig.getInstance().getEnableSearchServiceEsAppidSwitch()) {
            if (SSContext.isRatePlanRQ()) {
                if (cRequest != null) {
                    indexedTags.put("ss-client-id", cRequest.clientAppId());
                }
            } else {
                indexedTags.put("ss-client-id", hotelCalendarSearchRequest.getPublicSearchParameter().getSsClientAppId());
            }
        }
        if (SearchServiceConfig.getInstance().getEnableESRecordGroupId()) {
            indexedTags.put("groupid", Foundation.group().getId());
        }
        if (SearchServiceConfig.getInstance().getIsEnableLogToCatWithStoredTags()
                && cRequest != null) {
            if (SearchServiceConfig.getInstance().getIsEnableLogToCatWithAllHeaders()) {
                for (String key : cRequest.requestHeaderNames()) {
                    storedTags.put(key, cRequest.getHeader(key));
                }
            }
            // Clog????????????ID
            String clogTraceID = "";
            if (cRequest != null) {
                clogTraceID = (cRequest.getHeader("CLOGGING_TRACE_ID") != null) ? cRequest.getHeader("CLOGGING_TRACE_ID") : cRequest.getHeader("Tracing-TraceId");
            }
            storedTags.put("clog-trace-id", (clogTraceID != null) ? clogTraceID : "");
            // Cat????????????ID
            if (catProxyCheckServiceCall(cRequest)) {
                MessageTree catTree = Cat.getManager().getThreadLocalMessageTree();
                if (catTree != null) {
                    storedTags.put("cat-root-id", catTree.getRootMessageId());
                    storedTags.put("cat-parent-id", catTree.getParentMessageId());
                    storedTags.put("messageid", catTree.getMessageId());
                }
            }
            if (responseInfo != null) {
                indexedTags.put("rotaryhotelcount", String.valueOf(responseInfo.getRotaryHotelCount()));
                indexedTags.put("rotaryroomcount", String.valueOf(responseInfo.getRotaryRoomCount()));
                indexedTags.put("rotarytotaltime", String.valueOf(responseInfo.getRotaryTotalTime()));
                storedTags.put("request", responseInfo.getRequestForLargeRotaryTotalTime());
                if (responseInfo.getResponseAllHotelCount() >= 0) {
                    storedTags.put("custom_redirect", responseInfo.getRedirectMsg());
                    if (SearchServiceConfig.getInstance().getAddESRoomStatusWithRequestList()) {
                        // ????????????????????????
                        storedTags.put("allroomstatus", StringUtils.trimToEmpty(responseInfo.getAllRoomStatus()));
                        storedTags.put("allroomstatus2", StringUtils.trimToEmpty(responseInfo.getAllRoomStatus2()));
                    } else {
                        if (responseInfo.getResponseAllHotelCount() == 1) {
                            // ????????????????????????
                            storedTags.put("allroomstatus", StringUtils.trimToEmpty(responseInfo.getAllRoomStatus()));
                            storedTags.put("allroomstatus2", StringUtils.trimToEmpty(responseInfo.getAllRoomStatus2()));
                        }
                    }
                    if (responseInfo.getResponseAllHotelCount() == 1) {
                        // ????????????????????????
                        storedTags.put("allroomstatus", StringUtils.trimToEmpty(responseInfo.getAllRoomStatus()));
                    }
                }
                if (!StringCommonUtils.isNullOrWhiteSpace(responseInfo.getAllHotelStatus())) {
                    storedTags.put("allhotelstatus", responseInfo.getAllHotelStatus());
                }
                if (responseInfo.getLoggerRoomPriceMessage() != null) {
                    int len = Math.min(loggerRoomPriceMessageKeys.length, responseInfo.getLoggerRoomPriceMessage().size());
                    for (int i = 0; i < len; i++) {
                        storedTags.put(loggerRoomPriceMessageKeys[i], responseInfo.getLoggerRoomPriceMessage().get(i));
                    }
                }
                if (!StringCommonUtils.isNullOrEmpty(responseInfo.getFreeRoomDebugInfo())) {
                    storedTags.put("freeroomdebuginfo", responseInfo.getFreeRoomDebugInfo());
                }
            }
            if (args != null) {
                if (!StringCommonUtils.isNullOrEmpty(args.getKeyWord())) {
                    storedTags.put("keywordtext", args.getKeyWord());
                }
                if (!StringCommonUtils.isNullOrEmpty(args.getHotelName())) {
                    storedTags.put("hotelnametext", args.getHotelName());
                }
                if (!StringCommonUtils.isNullOrEmpty(args.getFilterTicketPlatformPromotionIDList())) {
                    storedTags.put("usercouponlist", args.getFilterTicketPlatformPromotionIDList());
                }
                if (!StringCommonUtils.isNullOrEmpty(args.getPlatformPromotionIDList())) {
                    storedTags.put("filtercouponlist", args.getPlatformPromotionIDList());
                }
            }
        }
        if (SearchServiceConfig.getInstance().getEnableLogErrorFlagToES()) {
            // ?????????????????????????????????????????????
            storedTags.put("haserror", (SSContext.getTraceInfo() != null && SSContext.getTraceInfo().getCatchedExceptions() > 0) ? "T" : "F");
        }
        if (SearchServiceConfig.getInstance().getLogCatBlockedKeyNames() != null && CollectionUtils.isNotEmpty(SearchServiceConfig.getInstance().getLogCatBlockedKeyNames())) {
            for (String key : SearchServiceConfig.getInstance().getLogCatBlockedKeyNames()) {
                if (indexedTags.containsKey(key)) {
                    indexedTags.remove(key);
                }
                if (storedTags.containsKey(key)) {
                    storedTags.remove(key);
                }
            }
        }
        if (SearchServiceConfig.getInstance().getEnableLogRequestToCat()) {
            String requestText = SSContext.getRequestText();
            String requestType = SSContext.getRequestType();
            if (StringCommonUtils.isNullOrWhiteSpace(requestText)) {
                storedTags.put("rqtype", StringUtils.trimToEmpty(requestType));
                storedTags.put("rqtext", StringUtils.trimToEmpty(requestText));
            }
        }
        if (SSContext.getTraceInfo() != null) {
            storedTags.put("computehotelcount", String.valueOf(SSContext.getTraceInfo().getComputeHotelCount()));
            storedTags.put("calledapicount", String.valueOf(SSContext.getTraceInfo().getCalledApiCount()));
        }
        if (SearchServiceConfig.getInstance().getIsMultiDayNewLogicV2UseMultiDayStatus()) {
            if (SSContext.getTraceInfo() != null) {
                indexedTags.put("roominfocopycount", String.valueOf(SSContext.getTraceInfo().getRoomInfoCopyTimes()));
            } else {
                indexedTags.put("roominfocopycount", "0");
            }
        }
        if (debugEntity != null) {
            indexedTags.put("loadmasterhoteltime", String.valueOf((debugEntity.getLoadHotelCost_LoadMaster() * 1000)));
            indexedTags.put("filtermasterhoteltime", String.valueOf((debugEntity.getLoadHotelCost_PreStaticFilter() * 1000)));
            indexedTags.put("filtersubhoteltime", String.valueOf((debugEntity.getLoadHotelCost_ProcessMaster() * 1000)));

            indexedTags.put("hotelsorttime", String.valueOf((debugEntity.getSortHotelTime() * 1000)));
            indexedTags.put("heapsorttime", String.valueOf((debugEntity.getHeapSortTime() * 1000)));
            indexedTags.put("gethoteltime", String.valueOf((debugEntity.getGetCacheData() * 1000)));
            indexedTags.put("af_h_filter", String.valueOf(debugEntity.getWowcomputehotelcount()));
            indexedTags.put("af_r_filter", String.valueOf(debugEntity.getWowcomputeroomcount()));
            indexedTags.put("bf_h_filter", String.valueOf(debugEntity.getWowhotelcount()));


            if (SSContext.getBusinessConfig().getEnableHotelListRecordErrorcode()){
                if (debugEntity.isSingleHotelMark()) {
                    indexedTags.put("hotelname","singlehotel");
                    if (!com.ctrip.hotel.product.search.cache.common.util.StringCommonUtils.isNullOrWhiteSpace(errorCode)
                            && debugEntity.getWowcomputeroomcount() == 0) {
                        indexedTags.put("errorcode", errorCode);
                    }
                }
            }




            indexedTags.put("computesubhotelcount", String.valueOf(debugEntity.getComputeSubHotelCount()));
            indexedTags.put("lazyloadroomcosttime", String.valueOf(debugEntity.getLazyLoadRoomCost() * 1000));
            indexedTags.put("dystep1", String.valueOf(debugEntity.getFilterTime() * 1000));
            indexedTags.put("dystep2", String.valueOf(debugEntity.getDynamicProcess_ProcessExCost() * 1000));
            indexedTags.put("bdbstatus", String.valueOf(debugEntity.getBDBStatus()));
            indexedTags.put("dbsoastatus", String.valueOf(debugEntity.getDBSOAStatus()));

            indexedTags.put("interval_rotatereform", String.valueOf(debugEntity.getInterval_rotatereform()));

            indexedTags.put("totalroomcount", String.valueOf(debugEntity.getTotalRoomCount()));
            indexedTags.put("outputroomcount", String.valueOf(debugEntity.getOutputRoomCount()));
            indexedTags.put("roomoutputrate", String.valueOf(debugEntity.getRoomOutputRate()));

            if (SearchServiceConfig.getInstance().getIsEnableLSLogInfo() && debugEntity.getCallSoaHotelCount() > 0) {
                indexedTags.put("callsoahotelcount", String.valueOf(debugEntity.getCallSoaHotelCount()));
                indexedTags.put("callsoacount", String.valueOf(debugEntity.getCallSoaCount()));
                indexedTags.put("lsfilterratio", String.valueOf(debugEntity.getLsFilterRatio()));
                indexedTags.put("callsoatimeout4j", String.valueOf(debugEntity.isCallSoaTimeOut4J() ? "T" : "F"));
                indexedTags.put("callsoatimeoutcount", String.valueOf(debugEntity.getCallSoaTimeOutCount()));
                indexedTags.put("callsoafinaltimeoutcount", String.valueOf(debugEntity.getCallSoaFinalTimeOutCount()));
                if (!StringCommonUtils.isNullOrEmpty(debugEntity.getCallSoaHotelList())) {
                    indexedTags.put("callsoahotellist", String.valueOf(debugEntity.getCallSoaHotelList()));
                }

                setCallSoaNotCanBook(debugEntity,indexedTags);

                if (!StringCommonUtils.isNullOrEmpty(debugEntity.getCallSoaTimeOutHotelList())) {
                    indexedTags.put("callsoatimeouthotellist", String.valueOf(debugEntity.getCallSoaTimeOutHotelList()));
                }

                if (!StringCommonUtils.isNullOrEmpty(debugEntity.getFilterItems())) {
                    indexedTags.put("filteritems", String.valueOf(debugEntity.getFilterItems()));
                }
                if (SearchServiceConfig.getInstance().getEnableLSTopAndBottomInfo()) {
                    indexedTags.put("forcetop", String.valueOf(debugEntity.getForceTopHotelList()));
                    indexedTags.put("forcebottom", String.valueOf(debugEntity.getForceBottomHotelList()));
                    indexedTags.put("rqmd5", String.valueOf(debugEntity.getRqMd5()));

                    indexedTags.put("outputhotels", String.valueOf(debugEntity.getOutputHotels()));
                }
            }
        } else {
            indexedTags.put("hotelname", "DebugEntityNull");
        }
        if (debugEntity != null && debugEntity.getInterfaceRedisInfo() != null) {
            InterfaceRedisMetric info = debugEntity.getInterfaceRedisInfo();
            indexedTags.put("interfaceredis_totaltime", String.valueOf(info.getTotalTime()));
            indexedTags.put("interfaceredis_readtimes", String.valueOf(info.getReadTimes()));
            indexedTags.put("interfaceredis_readredistime", String.valueOf(info.getReadRedisiTime()));
            indexedTags.put("interfaceredis_callpricingtime", String.valueOf(info.getCallPricingTime()));
            indexedTags.put("interfaceredis_callriskctrltime", String.valueOf(info.getCallRiskCtrlTime()));
            indexedTags.put("interfaceredis_mergetime", String.valueOf(info.getMergeTime()));
        }
        ConcurrentMap<String, TimeCostEntity> timeCostDic = ContextKeyUtils.getContextItem(ContextItemKey.TimeCostDic, ConcurrentMap.class);
        if (timeCostDic != null && timeCostDic.size() > 0) {
            Map<String, String> hotelShardIndexTags = new HashMap<String, String>();
            Map<String, String> hotelShardStoreTags = new HashMap<String, String>();
            hotelShardIndexTags.put("tracelogid", hotelCalendarSearchRequest.getPublicSearchParameter().getLogId() != null ? hotelCalendarSearchRequest.getPublicSearchParameter().getLogId() : "");
            hotelShardIndexTags.put("interval", String.valueOf(elapsedMilliseconds));
            if (debugEntity != null) {
                hotelShardIndexTags.put("loadmasterhotel", String.valueOf((debugEntity.getLoadHotelCost_LoadMaster() * 1000)));
                hotelShardIndexTags.put("prestaticfilter", String.valueOf((debugEntity.getLoadHotelCost_PreStaticFilter() * 1000)));
                hotelShardIndexTags.put("findsubhotel", String.valueOf((debugEntity.getLHC_PM_FilterSubHotel() * 1000)));
            }
            long redisCacheTimeCost = 0;
            for (Map.Entry<String, TimeCostEntity> kv : timeCostDic.entrySet()) {
                if (kv.getValue().isIndexTag()) {
                    String indexStr = "interval_" + kv.getKey().toLowerCase();
                    hotelShardIndexTags.put(indexStr, String.valueOf(kv.getValue().getMaxCost()));
                }
                if (kv.getKey().startsWith(TimeCostConst.KeyCache)) {
                    redisCacheTimeCost += kv.getValue().getMaxCost();
                }
            }
            hotelShardIndexTags.put("interval_rediscache", String.valueOf(redisCacheTimeCost));
            hotelShardStoreTags.put("interval_detail", JsonUtil.stringify(timeCostDic.entrySet().stream().filter(p -> p.getValue() != null && p.getValue().getTotalCost() > 0)));
            Cat.logTags("hotel-shard-timecost", hotelShardIndexTags, hotelShardStoreTags);
        }

        double interval_redis_total = 0;
        double interval_redis_deserialize_total = 0;
        long redis_size_total = 0;
        int redis_count_total = 0;
        double interval_setdata_total = 0;
        for (Map.Entry<String, String> entry : SSContext.getCatIndexTags().entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }

            if (entry.getKey().startsWith("interval_redis_")) {
                double interval_redis_deserialize = NumberConverter.convertToDouble(entry.getValue(), 0);
                if (entry.getKey().endsWith("_deserialize")) {
                    interval_redis_deserialize_total += interval_redis_deserialize;
                } else {
                    interval_redis_total += interval_redis_deserialize;
                }
                continue;
            }

            if (entry.getKey().startsWith("redis_")) {
                if (entry.getKey().endsWith("_count")) {
                    redis_count_total += NumberConverter.parseInt(entry.getValue(), 0);
                } else if (entry.getKey().endsWith("_size")) {
                    redis_size_total += com.ctrip.hotel.product.search.cache.common.util.StringCommonUtils.parseLong(entry.getValue(), 0);
                }
            }

            if (entry.getKey().startsWith("interval_setdata_")) {
                double interval_setdata_time = NumberConverter.convertToDouble(entry.getValue(), 0);
                interval_setdata_total += interval_setdata_time;
            }
        }
        SSContext.getCatIndexTags().put("interval_redis_total", String.valueOf(interval_redis_total));
        SSContext.getCatIndexTags().put("interval_redis_deserialize_total", String.valueOf(interval_redis_deserialize_total));
        SSContext.getCatIndexTags().put("redis_count_total", String.valueOf(redis_count_total));
        SSContext.getCatIndexTags().put("redis_size_total", String.valueOf(redis_size_total));
        SSContext.getCatIndexTags().put("interval_setdata_total", String.valueOf(interval_setdata_total));

        if (SearchServiceConfig.getInstance().getUseTaskBitFlag()) {
            TaskBitFlagWriter.writeToES(indexedTags);
        }

        if (SearchServiceConfig.getInstance().getRecordTimeCostForRedis()) {
            Map<String, VmsReadRedisTimeCostEntity> vmsReadRedisTimeCostMap = SSContext.getReadRedisTimeCost();
            if (vmsReadRedisTimeCostMap != null && vmsReadRedisTimeCostMap.size() > 0) {
                double total = 0;
                indexedTags.put("interval_redis_roomabase", "0.0");
                indexedTags.put("interval_redis_roombase_size", "0");
                indexedTags.put("interval_redis_roombase_roomcount", "0");
                indexedTags.put("roombase_count", "0");
                indexedTags.put("interval_redis_hotelbase", "0.0");
                indexedTags.put("interval_redis_hotelbase_size", "0");
                indexedTags.put("hotelbase_count", "0");
                indexedTags.put("interval_redis_hotelextra", "0.0");
                indexedTags.put("interval_redis_hotelextra_size", "0");
                indexedTags.put("hotelextra_count", "0");
                indexedTags.put("interval_redis_singleroomprice", "0.0");
                indexedTags.put("interval_redis_singleroomprice_readcount", "0");
                indexedTags.put("interval_redis_multiroomprice", "0.0");
                indexedTags.put("interval_redis_multiroomprice_readcount", "0");
                indexedTags.put("interval_redis_otherkeycache", "0.0");
                indexedTags.put("interval_desc_roombase", "0.0");
                indexedTags.put("interval_desc_hotelbase", "0.0");
                indexedTags.put("interval_desc_hotelextra", "0.0");
                indexedTags.put("interval_desc_singleroomprice", "0.0");
                indexedTags.put("interval_desc_multiroomprice", "0.0");
                indexedTags.put("interval_desc_otherkeycache", "0.0");

                for (Map.Entry<String, VmsReadRedisTimeCostEntity> kv : vmsReadRedisTimeCostMap.entrySet()) {
                    if (kv.getValue().isIndexTag()) {
                        total = total + kv.getValue().getTotalCost();
                        indexedTags.put(kv.getKey(), String.valueOf(kv.getValue().getTotalCost() * 1000));
                        if (StringCommonUtils.isEquals("interval_redis_roomabase", kv.getKey())) {
                            indexedTags.put("interval_redis_roombase_size", String.valueOf(kv.getValue().getMaxSize()));
                            // ??????????????? ?????????redis?????????????????????
                            indexedTags.put("interval_redis_roomabase_max", String.valueOf(kv.getValue().getMaxCost() * 1000));
                            if (kv.getValue().getCount() > 0) {
                                indexedTags.put("interval_redis_roomabase_avg", String.valueOf(kv.getValue().getTotalCost() / kv.getValue().getCount() * 1000));
                            }
                            indexedTags.put("roombase_count", String.valueOf(kv.getValue().getCount()));
                        }

                        if (StringCommonUtils.isEquals("interval_desc_roombase", kv.getKey())) {
                            indexedTags.put("interval_redis_roombase_roomcount", String.valueOf(kv.getValue().getRoomCount()));
                            // ??????????????? ???????????????????????????????????????
                            indexedTags.put("interval_desc_roombase_max", String.valueOf(kv.getValue().getMaxCost() * 1000));
                            if (kv.getValue().getCount() > 0) {
                                indexedTags.put("interval_desc_roombase_avg", String.valueOf(kv.getValue().getTotalCost() / kv.getValue().getCount() * 1000));
                            }
                        }

                        if (StringCommonUtils.isEquals("interval_redis_hotelbase", kv.getKey())) {
                            indexedTags.put("interval_redis_hotelbase_size", String.valueOf(kv.getValue().getMaxSize()));
                            indexedTags.put("hotelbase_count", String.valueOf(kv.getValue().getCount()));
                        }

                        if (StringCommonUtils.isEquals("interval_redis_hotelextra", kv.getKey())) {
                            indexedTags.put("interval_redis_hotelextra_size", String.valueOf(kv.getValue().getMaxSize()));
                            indexedTags.put("hotelextra_count", String.valueOf(kv.getValue().getCount()));
                        }

                        if (StringCommonUtils.isEquals("interval_redis_singleroomprice", kv.getKey())) {
                            indexedTags.put("interval_redis_singleroomprice_readcount", String.valueOf(kv.getValue().getCount()));
                        }

                        if (StringCommonUtils.isEquals("interval_redis_multiroomprice", kv.getKey())) {
                            indexedTags.put("interval_redis_singleroomprice_readcount", String.valueOf(kv.getValue().getCount()));
                        }
                    }
                }
                indexedTags.put("interval_redis_desc_total", String.valueOf(total * 1000));
            }
        }

        if (SearchServiceConfig.getInstance().getRecordEsUpStreamAppID()) {
            String upstreamAppID = responseInfo != null ? responseInfo.getUpStreamAppID() : "-1";
            indexedTags.put("upstreamappid", upstreamAppID);
        }
        if (SearchServiceConfig.getInstance().getEsRecordServiceType()) {
            indexedTags.put("servicetype", responseInfo != null ? responseInfo.getServiceType() : "1");
        }
        if (SearchServiceConfig.getInstance().getEnableDebugEntityLocalDateTime()) {
            indexedTags.put("checkinoffset", responseInfo != null ? String.valueOf(responseInfo.getCheckinoffset()) : "-99");
        }
        if (SSContext.getBusinessConfig().getEnableRecordCheckinOffsetDaysToInt()) {
            indexedTags.put("checkin_offset_days", responseInfo != null ? String.valueOf(responseInfo.getCheckinoffset()) : "-99");
        }



        if (SearchServiceConfig.getInstance().getRecordFilterRoomSupplierRemoveRooms()) {
            if (SearchServiceConfig.getInstance().getRecordFilterRoomSupplierRemoveRoomInfos()) {
                indexedTags.put("filterroomsupplier", responseInfo != null && responseInfo.getFilterRoomSupplierAllRemoveRoomIdList() != null
                        ? StringCommonUtils.join(responseInfo.getFilterRoomSupplierAllRemoveRoomIdList()) : "");
            }
            indexedTags.put("filterroomsuppliercount", responseInfo != null && responseInfo.getFilterRoomSupplierAllRemoveRoomIdList() != null
                    ? String.valueOf(responseInfo.getFilterRoomSupplierAllRemoveRoomIdList().size()) : "0");
            indexedTags.put("filterroomsuppliernum", responseInfo != null && responseInfo.getFilterRoomSupplierAllRemoveRoomIdList() != null
                    ? String.valueOf(responseInfo.getFilterRoomSupplierAllRemoveRoomIdList().size()) : "0");
        }
        if (SearchServiceConfig.getInstance().getEnableHotelListkClickHouseSplit()) {
            updateSSContextCatTagsV2(indexedTags, hotelCalendarSearchRequest,responseInfo);
        }
        removeCKField(indexedTags);
        updateSSContextCatTags(indexedTags, storedTags);
    }

    private void updateSSContextCatTags(Map<String, String> indexedTags, Map<String, String> storedTags) {
        if (SearchServiceConfig.getInstance().getFixRecordSearchEs()) {
            indexedTags.putAll(SSContext.getCatIndexTags());
            if (!SearchServiceConfig.getInstance().getLogCatAtLast()) {
                Cat.logTags("hotel-searchservice", indexedTags, storedTags);
            } else if (HttpRequestContext.getInstance() != null) {
                SSContext.setCatIndexTags(indexedTags);
                SSContext.setCatStoredTags(storedTags);
            }
        } else {
            indexedTags.putAll(SSContext.getCatIndexTags());
            if (SearchServiceConfig.getInstance().getLogCatAtLast()) {
                GlobalFunctions.recordSearchEs(indexedTags, null, storedTags);
            } else if (HttpRequestContext.getInstance() != null) {
                SSContext.setCatIndexTags(indexedTags);
                SSContext.setCatStoredTags(storedTags);
            }
        }
    }

    private void updateSSContextCatTagsV2(Map<String, String> indexedTags, SearchHotelDataRequest searchHotelDataRequest,DebugEntity debugEntity, CatLogStruct responseInfo) {
        if (indexedTags == null) {
            return;
        }

        try {

            HashMap<String, String> indexTagsV2 = new HashMap<>(120);
            indexTagsV2.putAll(SSContext.getCatIndexTagsV2());
            indexTagsV2.put("clientid", indexedTags.get("clientid"));
            indexTagsV2.put("checkindate", indexedTags.get("checkindate"));
            indexTagsV2.put("checkoutdate", indexedTags.get("checkoutdate"));
            indexTagsV2.put("pageindex", indexedTags.get("pageindex"));
            indexTagsV2.put("hotelcount", indexedTags.get("hotelcount"));

            indexTagsV2.put("checkinoffset", indexedTags.get("checkinoffset"));
            indexTagsV2.put("checkindeviation", indexedTags.get("checkinoffset"));


            indexTagsV2.put("staydays", indexedTags.get("range"));
            indexTagsV2.put("intervals", indexedTags.get("interval"));
            indexTagsV2.put("country", indexedTags.get("country"));
            indexTagsV2.put("city", indexedTags.get("city"));

            indexTagsV2.put("searchtype", indexedTags.get("searchtype"));
            indexTagsV2.put("channellistset", indexedTags.get("channellistset"));
            indexTagsV2.put("ordername", indexedTags.get("ordername"));
            indexTagsV2.put("ordertype", indexedTags.get("ordertype"));
            indexTagsV2.put("bf_h_filter", indexedTags.get("bf_h_filter"));

            indexTagsV2.put("af_h_filter", indexedTags.get("af_h_filter"));
            indexTagsV2.put("af_r_filter", indexedTags.get("af_r_filter"));
            indexTagsV2.put("fillrate", indexedTags.get("fillrate"));
            indexTagsV2.put("lsfilterratio", indexedTags.get("lsfilterratio"));
            indexTagsV2.put("inputhotels", indexedTags.get("hotellist"));

            indexTagsV2.put("rs_hotels", indexedTags.get("rs_hotels"));
            indexTagsV2.put("outputhotels", indexedTags.get("outputhotels"));
            indexTagsV2.put("callsoacount", indexedTags.get("callsoacount"));
            indexTagsV2.put("timeoutcount_retry", indexedTags.get("callsoafinaltimeoutcount"));
            indexTagsV2.put("callsoahotelcount", indexedTags.get("callsoahotelcount"));

            indexTagsV2.put("callsoahotellist", indexedTags.get("callsoahotellist"));
            indexTagsV2.put("timeout_total", indexedTags.get("callsoatimeout4j"));
            indexTagsV2.put("timeoutcount_single", indexedTags.get("callsoatimeoutcount"));
            indexTagsV2.put("canbookhotelnum", indexedTags.get("canbookhotelnum"));
            indexTagsV2.put("canbookroomnum", indexedTags.get("canbookroomnum"));

            indexTagsV2.put("notcanbookhotelnum", indexedTags.get("notcanbookhotelnum"));
            indexTagsV2.put("notcanbookroomnum", indexedTags.get("notcanbookroomnum"));
            indexTagsV2.put("dotx", indexedTags.get("dotx"));
            indexTagsV2.put("dotx2", indexedTags.get("dotx2"));
            indexTagsV2.put("doty", indexedTags.get("doty"));

            indexTagsV2.put("doty2", indexedTags.get("doty2"));
            indexTagsV2.put("group", indexedTags.get("group"));
            indexTagsV2.put("hitsearchlogic", indexedTags.get("hitsearchlogic"));
            indexTagsV2.put("invoicetargettype", indexedTags.get("invoicetargettype"));
            indexTagsV2.put("iscanreserve", indexedTags.get("iscanreserve"));

            indexTagsV2.put("keyword", indexedTags.get("keyword"));
            indexTagsV2.put("locale", indexedTags.get("locale"));
            indexTagsV2.put("maxlat", indexedTags.get("maxlat"));
            indexTagsV2.put("minlat", indexedTags.get("minlat"));
            indexTagsV2.put("maxlng", indexedTags.get("maxlng"));
            indexTagsV2.put("pricearea", indexedTags.get("pricearea"));

            String paytype = "";
            if (trueValue.equalsIgnoreCase(indexedTags.get("onlyfgprice"))) {
                paytype = "FG";
            }
            if (trueValue.equalsIgnoreCase(indexedTags.get("onlyppprice"))) {
                paytype = "PP";
            }
            if (trueValue.equalsIgnoreCase(indexedTags.get("onlyppprice")) && trueValue.equalsIgnoreCase(indexedTags.get("onlyfgprice"))) {
                paytype = "PPFG";
            }
            if (searchHotelDataRequest.getPublicSearchParameter().getFilterBySupportAnticipation()) {
                paytype = paytype + "??????";
            }


            indexTagsV2.put("minlng", indexedTags.get("minlng"));
            indexTagsV2.put("metro", indexedTags.get("metro"));
            // ?????????????????????
            indexTagsV2.put("paytype", paytype);
            indexTagsV2.put("queryscence", indexedTags.get("queryscence"));
            indexTagsV2.put("radius", indexedTags.get("radius"));

            String star = "";
            if (indexedTags.get("star") != null && indexedTags.get("star") != "" && !"-1".equalsIgnoreCase(indexedTags.get("star"))) {
                star = indexedTags.get("star");
            }
            if (indexedTags.get("starlist") != null && indexedTags.get("starlist") != "") {
                star = indexedTags.get("starlist");
            }
            if (indexedTags.get("starlist") != null && indexedTags.get("starlist") != "" && indexedTags.get("star") != null && indexedTags.get("star") != "") {
                if ("-1".equalsIgnoreCase(indexedTags.get("star"))) {

                } else {
                    star = indexedTags.get("star") + ";" + indexedTags.get("starlist");
                }
            }

            String[] starArray = star.split(",");
            Arrays.sort(starArray);
            star = String.valueOf(star);


            indexTagsV2.put("rq_md5", indexedTags.get("rqmd5"));
            indexTagsV2.put("spider", indexedTags.get("spider"));
            indexTagsV2.put("star", star);
            indexTagsV2.put("filtermasterhoteltime", indexedTags.get("filtermasterhoteltime"));

            indexTagsV2.put("filtersubhoteltime", indexedTags.get("filtersubhoteltime"));
            indexTagsV2.put("gethoteltime", indexedTags.get("gethoteltime"));
            indexTagsV2.put("heapsorttime", indexedTags.get("heapsorttime"));
            indexTagsV2.put("hotelsorttime", indexedTags.get("hotelsorttime"));
            indexTagsV2.put("loadmasterhoteltime", indexedTags.get("loadmasterhoteltime"));

            indexTagsV2.put("clientip", indexedTags.get("clientip"));
            indexTagsV2.put("uid", indexedTags.get("uid"));
            indexTagsV2.put("tracelogid", indexedTags.get("tracelogid"));


            // ???????????????????????????
            String hotelFeatureFilterList = "";
            String multiDimensionUnion = "";
            String featureListGroup2Filter = "";
            String hotelBrandListFilter = "";
            String bedTypeFilter = "";
            String basicRoomSpecialTagListFilter = "";

            String filterPropertyValueCodeList = "";
            String propertyCodeListFilter = "";
            String breakfast = "";
            String facilityFilter = "";
            String basicRoomFacilityListFilter = "";
            String hRatingOverallFilter = "";
            String lowNoVotersFilter = "";

            String filterReserveInvoice = "";

            String filterApplicativeAreaBlack = "";
            String filterByForeignGuestCodes = "";
            String filterCredentialsUnionWhite = "";

            String pOIKeyWordIDFilter = "";
            String personNumFilter = "";
            String roomQuantityFilter = "";

            String isctripgroupmembership = "";
            String promotiongroupunionfilterlist = "";
            String filterprepaydiscounttagidlist = "";
            String haspromotion = "";
            String ratecodelist = "";
            String filterratecodelist = "";

            String filterpropertyvalueidlist = "";
            String filterpropertycodelist = "";

            String bedTypeFilterTemp = "";
            pOIKeyWordIDFilter = searchHotelDataRequest.getPersonaInfoEntity() != null ? searchHotelDataRequest.getPersonaInfoEntity().getPoiKeyWord() != null ? searchHotelDataRequest.getPersonaInfoEntity().getPoiKeyWord().getId() : "" : "";
            PublicParametersEntity ppe = searchHotelDataRequest.getPublicSearchParameter();
            if (ppe != null) {
                if (SearchServiceConfig.getInstance().getEnableRecordRateCode()) {
                    ratecodelist = ppe.getRateCodeList();
                    filterratecodelist = ppe.getFilterRateCodeList();

                    filterpropertyvalueidlist = ppe.getFilterPropertyValueIDList();
                    filterpropertycodelist = ppe.getFilterPropertyCodeList();
                }

                hotelFeatureFilterList = ppe.getHotelFeatureFilterList() != null ? ppe.getHotelFeatureFilterList() : "";
                featureListGroup2Filter = ppe.getFeatureListGroup2() != null ? ppe.getFeatureListGroup2() : "";
                bedTypeFilter = ppe.getBedType() != null ? ppe.getBedType() : "";
                filterPropertyValueCodeList = ppe.getFilterPropertyValueCodeList() != null ? ppe.getFilterPropertyValueCodeList() : "";
                propertyCodeListFilter = ppe.getPropertyCodeList() != null ? ppe.getPropertyCodeList() : "";
                facilityFilter = ppe.getFacilityFilterList() != null ? ppe.getFacilityFilterList() + "," : facilityFilter;
                if (facilityFilter.startsWith("["))
                    facilityFilter = facilityFilter.substring(1, facilityFilter.length());
                if (facilityFilter.endsWith(","))
                    facilityFilter = facilityFilter.substring(0, facilityFilter.length() - 1);
                if (facilityFilter.endsWith("]"))
                    facilityFilter = facilityFilter.substring(0, facilityFilter.length() - 1);

                if (SearchServiceConfig.getInstance().getEnableRecordRateCode()) {
                    multiDimensionUnion = ppe.getMultiDimensionUnion();
                }

                hRatingOverallFilter = String.valueOf(ppe.getHRatingOverall());
                personNumFilter = String.valueOf(ppe.getPerson());
                try {
                    personNumFilter = personNumFilter + ppe.getFilterRoomByPerson() != null ? ppe.getFilterRoomByPerson().split(",")[0] : "";
                    ;
                } catch (Exception e) {

                }
                roomQuantityFilter = String.valueOf(ppe.getRoomQuantity());

                if (ppe.getCustomerTagList() != null) {
                    for (CustomerTagEntity customerTagEntity : ppe.getCustomerTagList()) {
                        if ("BasicRoomSpecialTagList".equalsIgnoreCase(customerTagEntity.getTagKey())) {
                            basicRoomSpecialTagListFilter = customerTagEntity.getTagValue();
                        }
                        // basicroomfacilitylistfilter
                        if ("BasicRoomFacilityList".equalsIgnoreCase(customerTagEntity.getTagKey())) {
                            basicRoomFacilityListFilter = customerTagEntity.getTagValue();
                        }
                        // basicroomfacilitylistfilter
                        if ("FilterReserveInvoice".equalsIgnoreCase(customerTagEntity.getTagKey())) {
                            filterReserveInvoice = customerTagEntity.getTagValue();
                        }
                        if (StringCommonUtils.isEquals(CustomerTagKeyConst.User_Biz_Scene, customerTagEntity.getTagKey())
                                && !StringCommonUtils.isNullOrWhiteSpace(customerTagEntity.getTagValue())) {
                            indexTagsV2.put("user_biz_scene", String.valueOf(customerTagEntity.getTagValue()));
                        }
                        isctripgroupmembership = "IsCtripGroupMembership".equalsIgnoreCase(customerTagEntity.getTagKey()) ? customerTagEntity.getTagValue() : isctripgroupmembership;
                        promotiongroupunionfilterlist = "PromotionGroupUnionFilterList".equalsIgnoreCase(customerTagEntity.getTagKey()) ? customerTagEntity.getTagValue() : promotiongroupunionfilterlist;
                        filterprepaydiscounttagidlist = "FilterPrepayDiscountTagIDList".equalsIgnoreCase(customerTagEntity.getTagKey()) ? customerTagEntity.getTagValue() : filterprepaydiscounttagidlist;
                        haspromotion = "HasPromotion".equalsIgnoreCase(customerTagEntity.getTagKey()) ? customerTagEntity.getTagValue() : haspromotion;
                    }
                }

                breakfast = ppe.getBreakfastNum();
                if (true == ppe.getBreakFast()) {
                    breakfast = breakfast + "true";
                }
            }
            if (searchHotelDataRequest.getFacilityEntity() != null) {
                hotelBrandListFilter = searchHotelDataRequest.getFacilityEntity().getHotelBrandList();

                bedTypeFilter = searchHotelDataRequest.getFacilityEntity().getHasTwinBed() == true ? bedTypeFilter + "TwinBed" : bedTypeFilter;
                bedTypeFilter = searchHotelDataRequest.getFacilityEntity().getHasKingSize() == true ? bedTypeFilter + "KingSize" : bedTypeFilter;

                facilityFilter = searchHotelDataRequest.getFacilityEntity().getOpenYear() > 0 ? facilityFilter + "OpenYear" + "," : facilityFilter;
                facilityFilter = searchHotelDataRequest.getFacilityEntity().getFitmentYear() > 0 ? facilityFilter + "FitmentYear" + "," : facilityFilter;

                facilityFilter = searchHotelDataRequest.getFacilityEntity().getPark() == true ? facilityFilter + "Park" + "," : facilityFilter;
                facilityFilter = searchHotelDataRequest.getFacilityEntity().getIsFreeWiredBroadnetFee() == true ? facilityFilter + "FreeWiredBroadnetFee" + "," : facilityFilter;
                facilityFilter = searchHotelDataRequest.getFacilityEntity().getIsFreeWirelessBroadnetFee() == true ? facilityFilter + "FreeWirelessBroadnetFee" + "," : facilityFilter;
                facilityFilter = searchHotelDataRequest.getFacilityEntity().getInnPet() == true ? facilityFilter + "InnPet" + "," : facilityFilter;
                facilityFilter = searchHotelDataRequest.getFacilityEntity().getIsFreePark() == true ? facilityFilter + "FreePark" + "," : facilityFilter;
                if (facilityFilter.endsWith(","))
                    facilityFilter = facilityFilter.substring(0, facilityFilter.length() - 1);
                lowNoVotersFilter = String.valueOf(searchHotelDataRequest.getFacilityEntity().getLowNoVoters());

            }

            indexTagsV2.put("breakfastfilter", breakfast);
            indexTagsV2.put("MultiDimensionUnion", multiDimensionUnion);
            indexTagsV2.put("hotelfeaturefilterlist", hotelFeatureFilterList);
            indexTagsV2.put("featurelistgroup2filter", featureListGroup2Filter);
            indexTagsV2.put("hotelbrandlistfilter", hotelBrandListFilter);
            indexTagsV2.put("bedtypefilter", bedTypeFilter);
            indexTagsV2.put("basicroomspecialtaglistfilter", basicRoomSpecialTagListFilter);

            indexTagsV2.put("filterpropertyvaluecodelist", filterPropertyValueCodeList);
            indexTagsV2.put("propertycodelistfilter", propertyCodeListFilter);
            indexTagsV2.put("facilityFilter", facilityFilter);
            indexTagsV2.put("basicRoomFacilityListFilter", basicRoomFacilityListFilter);
            indexTagsV2.put("hratingoverallfilter", hRatingOverallFilter);

            indexTagsV2.put("lownovotersfilter", lowNoVotersFilter);
            indexTagsV2.put("filterreserveinvoice", filterReserveInvoice);
            indexTagsV2.put("poikeywordidfilter", pOIKeyWordIDFilter);
            indexTagsV2.put("personnumfilter", personNumFilter);
            indexTagsV2.put("roomquantityfilter", roomQuantityFilter);

            indexTagsV2.put("isctripgroupmembership", isctripgroupmembership);
            indexTagsV2.put("promotiongroupunionfilterlist", promotiongroupunionfilterlist);
            indexTagsV2.put("filterprepaydiscounttagidlist", filterprepaydiscounttagidlist);
            indexTagsV2.put("haspromotion", haspromotion);

            if (SSContext.getBusinessConfig().getEnableRecordTimeCostToCK()) {
                Map<String, String> timeCostHashMap = Optional.ofNullable(responseInfo).map(x -> x.getTimeCostHashMap()).orElse(null);
                if (Optional.ofNullable(timeCostHashMap).isPresent()) {
                    indexTagsV2.put("realtimepricingavg_internal", timeCostHashMap.get("realtimepricingAvg"));
                    indexTagsV2.put("htlavailavg_internal", timeCostHashMap.get("htlavailAvg"));
                    indexTagsV2.put("ibufilteravg_internal", timeCostHashMap.get("ibufilterAvg"));
                }
            }

            if (debugEntity!=null && debugEntity.getUserFilterTagId() != null && debugEntity.getUserFilterTagId().size() > 0) {
                indexTagsV2.put("uesrtagIdList", debugEntity.getUserFilterTagId().stream().map(x -> "" + x).collect(Collectors.joining(","))); }

            if (SearchServiceConfig.getInstance().getEnableRecordRateCode()) {
                indexTagsV2.put("propertyvalueidlist", indexedTags.get("propertyvalueidlist"));
                indexTagsV2.put("ratecodelist", ratecodelist);
                indexTagsV2.put("filterratecodelist", filterratecodelist);


                indexTagsV2.put("filterpropertyvalueidlist", filterpropertyvalueidlist);
                indexTagsV2.put("filterpropertycodelist", filterpropertycodelist);
            }

            SSContext.setCatIndexTagsV2(indexTagsV2);

        } catch (Exception e) {
            LogHelper.getInstance().logError("hotel-listservice.ck", e);
        }

    }

    private void updateSSContextCatTagsV2(Map<String, String> indexedTags, HotelCalendarSearchRequest hotelCalendarSearchRequest, CatLogStruct responseInfo) {
        if (indexedTags == null) {
            return;
        }

        try {

            HashMap<String, String> indexTagsV2 = new HashMap<>(120);
            indexTagsV2.putAll(SSContext.getCatIndexTagsV2());
            indexTagsV2.put("clientid", indexedTags.get("clientid"));
            indexTagsV2.put("checkindate", indexedTags.get("checkindate"));
            indexTagsV2.put("checkoutdate", indexedTags.get("checkoutdate"));
            indexTagsV2.put("pageindex", indexedTags.get("pageindex"));
            indexTagsV2.put("hotelcount", indexedTags.get("hotelcount"));

            indexTagsV2.put("checkinoffset", indexedTags.get("checkinoffset"));
            indexTagsV2.put("checkin_offset_days", indexedTags.get("checkinoffset"));


            indexTagsV2.put("staydays", indexedTags.get("range"));// ?????????
            indexTagsV2.put("intervals", indexedTags.get("interval"));
            indexTagsV2.put("country", indexedTags.get("country"));
            indexTagsV2.put("city", indexedTags.get("city"));

            indexTagsV2.put("searchtype", indexedTags.get("searchtype"));
            indexTagsV2.put("channellistset", indexedTags.get("channellistset"));
            indexTagsV2.put("ordername", indexedTags.get("ordername"));
            indexTagsV2.put("ordertype", indexedTags.get("ordertype"));
            indexTagsV2.put("bf_h_filter", indexedTags.get("bf_h_filter"));

            indexTagsV2.put("af_h_filter", indexedTags.get("af_h_filter"));
            indexTagsV2.put("af_r_filter", indexedTags.get("af_r_filter"));
            indexTagsV2.put("fillrate", indexedTags.get("fillrate"));
            indexTagsV2.put("lsfilterratio", indexedTags.get("lsfilterratio"));
            indexTagsV2.put("inputhotels", indexedTags.get("hotellist"));// ?????????

            indexTagsV2.put("rs_hotels", indexedTags.get("rs_hotels"));
            indexTagsV2.put("outputhotels", indexedTags.get("outputhotels"));
            indexTagsV2.put("callsoacount", indexedTags.get("callsoacount"));
            indexTagsV2.put("timeoutcount_retry", indexedTags.get("callsoafinaltimeoutcount"));
            indexTagsV2.put("callsoahotelcount", indexedTags.get("callsoahotelcount"));

            indexTagsV2.put("callsoahotellist", indexedTags.get("callsoahotellist"));
            indexTagsV2.put("timeout_total", indexedTags.get("callsoatimeout4j"));
            indexTagsV2.put("timeoutcount_single", indexedTags.get("callsoatimeoutcount"));
            indexTagsV2.put("canbookhotelnum", indexedTags.get("canbookhotelnum"));
            indexTagsV2.put("canbookroomnum", indexedTags.get("canbookroomnum"));

            indexTagsV2.put("notcanbookhotelnum", indexedTags.get("notcanbookhotelnum"));
            indexTagsV2.put("notcanbookroomnum", indexedTags.get("notcanbookroomnum"));
            indexTagsV2.put("dotx", indexedTags.get("dotx"));
            indexTagsV2.put("dotx2", indexedTags.get("dotx2"));
            indexTagsV2.put("doty", indexedTags.get("doty"));

            indexTagsV2.put("doty2", indexedTags.get("doty2"));
            indexTagsV2.put("group", indexedTags.get("group"));
            indexTagsV2.put("hitsearchlogic", indexedTags.get("hitsearchlogic"));
            indexTagsV2.put("invoicetargettype", indexedTags.get("invoicetargettype"));
            indexTagsV2.put("iscanreserve", indexedTags.get("iscanreserve"));

            indexTagsV2.put("keyword", indexedTags.get("keyword"));
            indexTagsV2.put("locale", indexedTags.get("locale"));
            indexTagsV2.put("maxlat", indexedTags.get("maxlat"));
            indexTagsV2.put("minlat", indexedTags.get("minlat"));
            indexTagsV2.put("maxlng", indexedTags.get("maxlng"));
            indexTagsV2.put("pricearea", indexedTags.get("pricearea"));

            String paytype = "";
            if (trueValue.equalsIgnoreCase(indexedTags.get("onlyfgprice"))) {
                paytype = "FG";
            }
            if (trueValue.equalsIgnoreCase(indexedTags.get("onlyppprice"))) {
                paytype = "PP";
            }
            if (trueValue.equalsIgnoreCase(indexedTags.get("onlyppprice")) && trueValue.equalsIgnoreCase(indexedTags.get("onlyfgprice"))) {
                paytype = "PPFG";
            }
            if (hotelCalendarSearchRequest.getPublicSearchParameter().getFilterBySupportAnticipation()) {
                paytype = paytype + "??????";
            }


            indexTagsV2.put("minlng", indexedTags.get("minlng"));
            indexTagsV2.put("metro", indexedTags.get("metro"));
            indexTagsV2.put("paytype", paytype);// ?????????????????????
            indexTagsV2.put("queryscence", indexedTags.get("queryscence"));
            indexTagsV2.put("radius", indexedTags.get("radius"));

            String star = "";
            if (indexedTags.get("star") != null && indexedTags.get("star") != "" && !"-1".equalsIgnoreCase(indexedTags.get("star"))) {
                star = indexedTags.get("star");
            }
            if (indexedTags.get("starlist") != null && indexedTags.get("starlist") != "") {
                star = indexedTags.get("starlist");
            }
            if (indexedTags.get("starlist") != null && indexedTags.get("starlist") != "" && indexedTags.get("star") != null && indexedTags.get("star") != "") {
                if ("-1".equalsIgnoreCase(indexedTags.get("star"))) {

                } else {
                    star = indexedTags.get("star") + ";" + indexedTags.get("starlist");
                }
            }

            String[] starArray = star.split(",");
            Arrays.sort(starArray);
            star = String.valueOf(star);


            indexTagsV2.put("rq_md5", indexedTags.get("rqmd5"));
            indexTagsV2.put("spider", indexedTags.get("spider"));
            indexTagsV2.put("star", star);
            indexTagsV2.put("filtermasterhoteltime", indexedTags.get("filtermasterhoteltime"));

            indexTagsV2.put("filtersubhoteltime", indexedTags.get("filtersubhoteltime"));
            indexTagsV2.put("gethoteltime", indexedTags.get("gethoteltime"));
            indexTagsV2.put("heapsorttime", indexedTags.get("heapsorttime"));
            indexTagsV2.put("hotelsorttime", indexedTags.get("hotelsorttime"));
            indexTagsV2.put("loadmasterhoteltime", indexedTags.get("loadmasterhoteltime"));

            // indexTagsV2.put("ip", indexedTags.get("ip"));
            indexTagsV2.put("clientip", indexedTags.get("clientip"));
            indexTagsV2.put("uid", indexedTags.get("uid"));
            indexTagsV2.put("tracelogid", indexedTags.get("tracelogid"));


            // ???????????????????????????
            String hotelFeatureFilterList = "";
            String multiDimensionUnion = "";
            String featureListGroup2Filter = "";
            String hotelBrandListFilter = "";
            String bedTypeFilter = "";
            String basicRoomSpecialTagListFilter = "";

            String filterPropertyValueCodeList = "";
            String propertyCodeListFilter = "";
            String breakfast = "";
            String facilityFilter = "";
            String basicRoomFacilityListFilter = "";
            String hRatingOverallFilter = "";
            String lowNoVotersFilter = "";

            String filterReserveInvoice = "";

            String filterApplicativeAreaBlack = "";
            String filterByForeignGuestCodes = "";
            String filterCredentialsUnionWhite = "";

            String pOIKeyWordIDFilter = "";
            String personNumFilter = "";
            String roomQuantityFilter = "";

            String isctripgroupmembership = "";
            String promotiongroupunionfilterlist = "";
            String filterprepaydiscounttagidlist = "";
            String haspromotion = "";
            String ratecodelist = "";
            String filterratecodelist = "";

            String filterpropertyvalueidlist = "";
            String filterpropertycodelist = "";

            String bedTypeFilterTemp = "";
            pOIKeyWordIDFilter = hotelCalendarSearchRequest.getPersonaInfoEntity() != null ? hotelCalendarSearchRequest.getPersonaInfoEntity().getPoiKeyWord() != null ? hotelCalendarSearchRequest.getPersonaInfoEntity().getPoiKeyWord().getId() : "" : "";
            PublicParametersEntity ppe = hotelCalendarSearchRequest.getPublicSearchParameter();
            if (ppe != null) {
                if (SearchServiceConfig.getInstance().getEnableRecordRateCode()) {
                    ratecodelist = ppe.getRateCodeList();
                    filterratecodelist = ppe.getFilterRateCodeList();

                    filterpropertyvalueidlist = ppe.getFilterPropertyValueIDList();
                    filterpropertycodelist = ppe.getFilterPropertyCodeList();
                }

                hotelFeatureFilterList = ppe.getHotelFeatureFilterList() != null ? ppe.getHotelFeatureFilterList() : "";
                featureListGroup2Filter = ppe.getFeatureListGroup2() != null ? ppe.getFeatureListGroup2() : "";
                bedTypeFilter = ppe.getBedType() != null ? ppe.getBedType() : "";
                filterPropertyValueCodeList = ppe.getFilterPropertyValueCodeList() != null ? ppe.getFilterPropertyValueCodeList() : "";
                propertyCodeListFilter = ppe.getPropertyCodeList() != null ? ppe.getPropertyCodeList() : "";
                facilityFilter = ppe.getFacilityFilterList() != null ? ppe.getFacilityFilterList() + "," : facilityFilter;
                if (facilityFilter.startsWith("["))
                    facilityFilter = facilityFilter.substring(1, facilityFilter.length());
                if (facilityFilter.endsWith(","))
                    facilityFilter = facilityFilter.substring(0, facilityFilter.length() - 1);
                if (facilityFilter.endsWith("]"))
                    facilityFilter = facilityFilter.substring(0, facilityFilter.length() - 1);

                if (SearchServiceConfig.getInstance().getEnableRecordRateCode()) {
                    multiDimensionUnion = ppe.getMultiDimensionUnion();
                }

                hRatingOverallFilter = String.valueOf(ppe.getHRatingOverall());
                personNumFilter = String.valueOf(ppe.getPerson());
                try {
                    personNumFilter = personNumFilter + ppe.getFilterRoomByPerson() != null ? ppe.getFilterRoomByPerson().split(",")[0] : "";
                    ;
                } catch (Exception e) {

                }
                roomQuantityFilter = String.valueOf(ppe.getRoomQuantity());

                if (ppe.getCustomerTagList() != null) {
                    for (CustomerTagEntity customerTagEntity : ppe.getCustomerTagList()) {
                        if ("BasicRoomSpecialTagList".equalsIgnoreCase(customerTagEntity.getTagKey())) {
                            basicRoomSpecialTagListFilter = customerTagEntity.getTagValue();
                        }
                        // basicroomfacilitylistfilter
                        if ("BasicRoomFacilityList".equalsIgnoreCase(customerTagEntity.getTagKey())) {
                            basicRoomFacilityListFilter = customerTagEntity.getTagValue();
                        }
                        // basicroomfacilitylistfilter
                        if ("FilterReserveInvoice".equalsIgnoreCase(customerTagEntity.getTagKey())) {
                            filterReserveInvoice = customerTagEntity.getTagValue();
                        }

                        isctripgroupmembership = "IsCtripGroupMembership".equalsIgnoreCase(customerTagEntity.getTagKey()) ? customerTagEntity.getTagValue() : isctripgroupmembership;
                        promotiongroupunionfilterlist = "PromotionGroupUnionFilterList".equalsIgnoreCase(customerTagEntity.getTagKey()) ? customerTagEntity.getTagValue() : promotiongroupunionfilterlist;
                        filterprepaydiscounttagidlist = "FilterPrepayDiscountTagIDList".equalsIgnoreCase(customerTagEntity.getTagKey()) ? customerTagEntity.getTagValue() : filterprepaydiscounttagidlist;
                        haspromotion = "HasPromotion".equalsIgnoreCase(customerTagEntity.getTagKey()) ? customerTagEntity.getTagValue() : haspromotion;
                    }
                }

                breakfast = ppe.getBreakfastNum();
                if (true == ppe.getBreakFast()) {
                    breakfast = breakfast + "true";
                }
            }
            if (hotelCalendarSearchRequest.getFacilityEntity() != null) {
                hotelBrandListFilter = hotelCalendarSearchRequest.getFacilityEntity().getHotelBrandList();

                bedTypeFilter = hotelCalendarSearchRequest.getFacilityEntity().getHasTwinBed() == true ? bedTypeFilter + "TwinBed" : bedTypeFilter;
                bedTypeFilter = hotelCalendarSearchRequest.getFacilityEntity().getHasKingSize() == true ? bedTypeFilter + "KingSize" : bedTypeFilter;

                facilityFilter = hotelCalendarSearchRequest.getFacilityEntity().getOpenYear() > 0 ? facilityFilter + "OpenYear" + "," : facilityFilter;
                facilityFilter = hotelCalendarSearchRequest.getFacilityEntity().getFitmentYear() > 0 ? facilityFilter + "FitmentYear" + "," : facilityFilter;

                facilityFilter = hotelCalendarSearchRequest.getFacilityEntity().getPark() == true ? facilityFilter + "Park" + "," : facilityFilter;
                facilityFilter = hotelCalendarSearchRequest.getFacilityEntity().getIsFreeWiredBroadnetFee() == true ? facilityFilter + "FreeWiredBroadnetFee" + "," : facilityFilter;
                facilityFilter = hotelCalendarSearchRequest.getFacilityEntity().getIsFreeWirelessBroadnetFee() == true ? facilityFilter + "FreeWirelessBroadnetFee" + "," : facilityFilter;
                facilityFilter = hotelCalendarSearchRequest.getFacilityEntity().getInnPet() == true ? facilityFilter + "InnPet" + "," : facilityFilter;
                facilityFilter = hotelCalendarSearchRequest.getFacilityEntity().getIsFreePark() == true ? facilityFilter + "FreePark" + "," : facilityFilter;
                if (facilityFilter.endsWith(","))
                    facilityFilter = facilityFilter.substring(0, facilityFilter.length() - 1);
                lowNoVotersFilter = String.valueOf(hotelCalendarSearchRequest.getFacilityEntity().getLowNoVoters());

            }
            indexTagsV2.put("breakfastfilter", breakfast);
            indexTagsV2.put("MultiDimensionUnion", multiDimensionUnion);
            indexTagsV2.put("hotelfeaturefilterlist", hotelFeatureFilterList);
            indexTagsV2.put("featurelistgroup2filter", featureListGroup2Filter);
            indexTagsV2.put("hotelbrandlistfilter", hotelBrandListFilter);
            indexTagsV2.put("bedtypefilter", bedTypeFilter);
            indexTagsV2.put("basicroomspecialtaglistfilter", basicRoomSpecialTagListFilter);

            indexTagsV2.put("filterpropertyvaluecodelist", filterPropertyValueCodeList);
            indexTagsV2.put("propertycodelistfilter", propertyCodeListFilter);
            indexTagsV2.put("facilityFilter", facilityFilter);
            indexTagsV2.put("basicRoomFacilityListFilter", basicRoomFacilityListFilter);
            indexTagsV2.put("hratingoverallfilter", hRatingOverallFilter);

            indexTagsV2.put("lownovotersfilter", lowNoVotersFilter);
            indexTagsV2.put("filterreserveinvoice", filterReserveInvoice);
            indexTagsV2.put("poikeywordidfilter", pOIKeyWordIDFilter);
            indexTagsV2.put("personnumfilter", personNumFilter);
            indexTagsV2.put("roomquantityfilter", roomQuantityFilter);

            indexTagsV2.put("isctripgroupmembership", isctripgroupmembership);
            indexTagsV2.put("promotiongroupunionfilterlist", promotiongroupunionfilterlist);
            indexTagsV2.put("filterprepaydiscounttagidlist", filterprepaydiscounttagidlist);
            indexTagsV2.put("haspromotion", haspromotion);

            if (SSContext.getBusinessConfig().getEnableRecordTimeCostToCK()) {
                Map<String, String> timeCostHashMap = Optional.ofNullable(responseInfo).map(x -> x.getTimeCostHashMap()).orElse(null);
                if (Optional.ofNullable(timeCostHashMap).isPresent()) {
                    indexTagsV2.put("realtimepricingavg_internal", timeCostHashMap.get("realtimepricingAvg"));
                    indexTagsV2.put("htlavailavg_internal", timeCostHashMap.get("htlavailAvg"));
                    indexTagsV2.put("ibufilteravg_internal", timeCostHashMap.get("ibufilterAvg"));
                }
            }

            if (SearchServiceConfig.getInstance().getEnableRecordRateCode()) {
                indexTagsV2.put("propertyvalueidlist", indexedTags.get("propertyvalueidlist"));
                indexTagsV2.put("ratecodelist", ratecodelist);
                indexTagsV2.put("filterratecodelist", filterratecodelist);


                indexTagsV2.put("filterpropertyvalueidlist", filterpropertyvalueidlist);
                indexTagsV2.put("filterpropertycodelist", filterpropertycodelist);
            }

            SSContext.setCatIndexTagsV2(indexTagsV2);

        } catch (Exception e) {
            LogHelper.getInstance().logError("hotel-listservice.ck", e);
        }

    }

    /**
     * copy from:Arch.CFX.dll(1.7.6.0) namespace:Arch.CFramework.CatHelper.CatProxy method: private static bool
     * CheckServiceCall() {...}
     *
     * @param cRequest HttpRequest Instance
     * @return
     */
    public boolean catProxyCheckServiceCall(HttpRequestWrapper cRequest) {
        if (SearchServiceConfig.getInstance().getEnableStoredTagCatMessageId()) {
            return true;
        }
        cRequest = (cRequest == null && HttpRequestContext.getInstance() != null) ? HttpRequestContext.getInstance().request() : cRequest;
        if (cRequest == null) {
            return true;
        }
        return StringUtils.isNotBlank(cRequest.getHeader("CallApp"));
    }

    public void logDebugEntityToCat(SearchHotelDataRequest searchHotelDataRequest, long elapsedMilliseconds, DebugEntity debugEntity, String hitLogicIds, String cacheMode) {
        logDebugEntityToCat(searchHotelDataRequest, elapsedMilliseconds, debugEntity, hitLogicIds, cacheMode, null);
    }

    public void logDebugEntityToCat(SearchHotelDataRequest searchHotelDataRequest, long elapsedMilliseconds, DebugEntity debugEntity, String hitLogicIds) {
        logDebugEntityToCat(searchHotelDataRequest, elapsedMilliseconds, debugEntity, hitLogicIds, "", null);
    }

    public void logDebugEntityToCat(SearchHotelDataRequest searchHotelDataRequest, long elapsedMilliseconds, DebugEntity debugEntity) {
        logDebugEntityToCat(searchHotelDataRequest, elapsedMilliseconds, debugEntity, "", "", null);
    }

    public void logDebugEntityToCat(SearchHotelDataRequest searchHotelDataRequest, long elapsedMilliseconds, DebugEntity debugEntity, String hitLogicIds, String cacheMode, String errorCode) {
        if (!SearchServiceConfig.getInstance().getLogCat()) {
            return;
        }
        if (SearchServiceConfig.getInstance().getEnableSaveDebugEntityToCat() == false) {
            return;
        }
        Map<String, String> CatLogFields = new HashMap<>();

        if (SearchServiceConfig.getInstance().getEnablefixHotelSearchservicelongcost()) {
            // searchHotelDataRequest.getCatLogFields();
            CatLogFields = SSContext.getCatIndexTags();
        } else {
            // searchHotelDataRequest.getCatLogFields();
            CatLogFields = new HashMap<>();
        }


        int computeHotelCount = SSContext.getTraceInfo() != null ? SSContext.getTraceInfo().getComputeHotelCount() : 0;
        int computeRoomCount = 0;
        String computeRoomCountTemp = null;

        if (SearchServiceConfig.getInstance().getEnablefixHotelSearchservicelongcost()) {
            if (SSContext.getCatIndexTags() != null) {
                // searchHotelDataRequest.getCatLogFields();
                CatLogFields = SSContext.getCatIndexTags();
            }
        } else {
            // searchHotelDataRequest.getCatLogFields();
            CatLogFields = new HashMap<>();
        }

        if (CatLogFields.containsKey("computeroomnum")) {
            computeRoomCountTemp = CatLogFields.get("computeroomnum");
            if (StringUtils.isNumeric(computeRoomCountTemp)) {
                computeRoomCount = Integer.parseInt(computeRoomCountTemp);
            }
        }
        // ?????????LongCostES
        if (elapsedMilliseconds < SearchServiceConfig.getInstance().getDebugEntityToCatIntervalLimit()
                && computeHotelCount < SearchServiceConfig.getInstance().getLongCallLimitComputeHotelCount()
                && computeRoomCount < SearchServiceConfig.getInstance().getLongCallLimitComputeRoomCount()) {
            return;
        }
        HashMap<String, String> indexedTags = new HashMap<>(31);
        HashMap<String, String> storedTags = new HashMap<>(2);
        if (SearchServiceConfig.getInstance().getEnableLogDebugEntityToProtobufBase64()) {
            String requestBase64 = null;

            if (SearchServiceConfig.getInstance().getEnablefixHotelSearchservicelongcost()) {
                String requestString = null;
                try {
                    requestString = JsonUtil.stringify(searchHotelDataRequest);
                    Base64 base64 = new Base64();
                    requestBase64 = base64.encodeToString(requestString.getBytes("UTF-8"));

                } catch (Exception e) {

                }
            }

            storedTags.put("request", requestBase64);
        } else {
            String requestString = JsonUtil.stringify(searchHotelDataRequest);
            storedTags.put("request", requestString);
        }
        if (debugEntity != null) {
            String debugEntityString = JsonUtil.stringify(debugEntity);
            storedTags.put("debugentity", debugEntityString);
            indexedTags.put("debugEntityInfo", String.valueOf(debugEntity.getTotalTime())); // ???????????????
            indexedTags.put("1_loadhoteltime", String.valueOf(debugEntity.getGetCacheData())); // ?????????????????????
            indexedTags.put("1_processchaintime", String.valueOf(debugEntity.getProcessChainTime())); // ????????????????????????
            indexedTags.put("2_lazydynamicprocesscost", String.valueOf(debugEntity.getLazyDynamicProcessCost())); // ??????lazy???????????????
            storedTags.put("readinterfaceredistimes", String.valueOf(debugEntity.getReadInterfaceRedisTimes()));
            storedTags.put("readinterfaceRedisTotalTime", String.valueOf(debugEntity.getReadInterfaceRedisTotalTime()));
            storedTags.put("callpricingtimes", String.valueOf(debugEntity.getCallPricingTimes()));
            storedTags.put("callpricingtotaltime", String.valueOf(debugEntity.getCallPricingTotalTime()));
            storedTags.put("callriskcontroltimes", String.valueOf(debugEntity.getCallRiskControlTimes()));
            storedTags.put("callriskcontroltotaltime", String.valueOf(debugEntity.getCallRiskControlTotalTime()));
            storedTags.put("callinterfacetotaltime", String.valueOf(debugEntity.getCallInterfaceTotalTime()));
            storedTags.put("callinterfacetimes", String.valueOf(debugEntity.getCallInterfaceTimes()));
            storedTags.put("descinterfaceredistotaltime", String.valueOf(debugEntity.getDescInterfaceRedisTotalTime()));
            storedTags.put("makepricingreqtotaltime", String.valueOf(debugEntity.getMakePricingReqTotalTime()));
            storedTags.put("processpricingresptotaltime", String.valueOf(debugEntity.getProcessPricingRespTotalTime()));
            storedTags.put("hitcachetimes", String.valueOf(debugEntity.getHitCacheTimes()));
            storedTags.put("ibufiltercount", String.valueOf(debugEntity.getIbufilter_count()));
            storedTags.put("ibufiltertimes", String.valueOf(debugEntity.getInterval_ibufilter()));
            if (debugEntity.getInterfaceRedisInfo() != null) {
                InterfaceRedisMetric info = debugEntity.getInterfaceRedisInfo();
                indexedTags.put("interfaceredis_totaltime", String.valueOf(info.getTotalTime()));
                indexedTags.put("interfaceredis_readtimes", String.valueOf(info.getReadTimes()));
                indexedTags.put("interfaceredis_readredistime", String.valueOf(info.getReadRedisiTime()));
                indexedTags.put("interfaceredis_callpricingtime", String.valueOf(info.getCallPricingTime()));
                indexedTags.put("interfaceredis_callriskctrltime", String.valueOf(info.getCallRiskCtrlTime()));
                indexedTags.put("interfaceredis_mergetime", String.valueOf(info.getMergeTime()));
            }
        }
        // #region Set Debug Entity
        if (CatLogFields.containsKey("clientid")) {
            indexedTags.put("clientid", (CatLogFields.get("clientid") != null) ? CatLogFields.get("clientid") : "");
        }
        if (CatLogFields.containsKey("clientip")) {
            indexedTags.put("clientip", (CatLogFields.get("clientip") != null) ? CatLogFields.get("clientip") : "");
        }
        if (CatLogFields.containsKey("cluster")) {
            indexedTags.put("cluster", (CatLogFields.get("cluster") != null) ? CatLogFields.get("cluster") : "");
        }
        if (CatLogFields.containsKey("ss-client-id")) {
            indexedTags.put("ss-client-id", (CatLogFields.get("ss-client-id") != null) ? CatLogFields.get("ss-client-id") : "");
        }
        // ??????
        indexedTags.put("interval", String.valueOf(elapsedMilliseconds));
        // ???????????????????????????
        indexedTags.put("hitsearchlogic", StringCommonUtils.isNullOrWhiteSpace(hitLogicIds) ? "0" : hitLogicIds);
        // ?????????????????????
        int pageIndex = searchHotelDataRequest.getSearchTypeEntity().getPageIndex();
        if (SearchServiceConfig.getInstance().getFixOverflowNumForSearchServiceLog() && pageIndex < 0) {
            pageIndex = 0;
        }
        indexedTags.put("pageindex", String.valueOf(pageIndex));
        // ????????????
        if (searchHotelDataRequest.getMapSearchEntity() != null) {
            indexedTags.put("dotx", String.valueOf(searchHotelDataRequest.getMapSearchEntity().getDotX()));
            indexedTags.put("dotx2", String.valueOf(searchHotelDataRequest.getMapSearchEntity().getDotX2()));
            indexedTags.put("doty", String.valueOf(searchHotelDataRequest.getMapSearchEntity().getDotY()));
            indexedTags.put("doty2", String.valueOf(searchHotelDataRequest.getMapSearchEntity().getDotY2()));
            indexedTags.put("maxlat", String.valueOf(searchHotelDataRequest.getMapSearchEntity().getMaxLat()));
            indexedTags.put("minlat", String.valueOf(searchHotelDataRequest.getMapSearchEntity().getMinLat()));
            indexedTags.put("maxlng", String.valueOf(searchHotelDataRequest.getMapSearchEntity().getMaxLng()));
            indexedTags.put("minlng", String.valueOf(searchHotelDataRequest.getMapSearchEntity().getMinLng()));
            indexedTags.put("radius", String.valueOf(searchHotelDataRequest.getMapSearchEntity().getRadius()));
        } else {
            indexedTags.put("radius", "0");
        }
        indexedTags.put("computehotelscount", String.valueOf(computeHotelCount)); // ???????????????
        indexedTags.put("computeRoomCount", String.valueOf(computeRoomCount)); // ???????????????
        indexedTags.put("tracelogid", (searchHotelDataRequest.getPublicSearchParameter().getLogId() != null) ? searchHotelDataRequest.getPublicSearchParameter().getLogId() : "");
        if (SearchServiceConfig.getInstance().getEnableHotelSearchservicelongcostAddRecord()) {
            indexedTags.put("checkindate", DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(searchHotelDataRequest.getPublicSearchParameter().getCheckInDate()));
            indexedTags.put("checkoutdate", DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(searchHotelDataRequest.getPublicSearchParameter().getCheckOutDate()));
            indexedTags.put("range", String.valueOf(DateCommonUtils.getDaysBetween(searchHotelDataRequest.getPublicSearchParameter().getCheckInDate().getTime(), searchHotelDataRequest.getPublicSearchParameter().getCheckOutDate().getTime())));
        }
        // #endregion
        if (SearchServiceConfig.getInstance().getEnablefixHotelSearchservicelongcost()) {
            if (!SearchServiceConfig.getInstance().getLogCatAtLast()) {
                Cat.logTags("hotel-searchservicelongcostresponse", indexedTags, storedTags);
            } else {
                SSContext.setLongcallCatIndexTags(indexedTags);
                SSContext.setLongcallCatStoredTags(storedTags);
            }
        } else {
            // #endregion
            if (SearchServiceConfig.getInstance().getEnableSaveReqResInfoToLongCostCat()) {
                Cat.logTags("hotel-searchservicelongcostresponse", indexedTags, storedTags);
            }
        }
    }

    // ??????RequestXML???clog???
    public void logRequestToQMQ(SearchHotelDataRequest request, String appid, boolean isSingleHotel, String platform) {
        if (request == null ||
                request.getPublicSearchParameter() == null ||
                StringCommonUtils.isNullOrEmpty(request.getPublicSearchParameter().getLogId()) ||
                !isSingleHotel) {
            return;
        }

        try {

            if (!SSContext.isRatePlanRQ() && !SearchServiceConfig.getInstance().getIsOverseaCluster()) {
                boolean doNotSendAppId = SearchServiceConfig.getInstance().getQMQRecordDomesticRequestAPPID().size() == 0
                        || (!SearchServiceConfig.getInstance().getQMQRecordDomesticRequestAPPID().contains(appid)
                        && !SearchServiceConfig.getInstance().getQMQRecordDomesticRequestAPPID().contains("-100"));

                boolean doNotSendPlatform = SearchServiceConfig.getInstance().getQMQRecordDomesticRequestPlatform() == null
                        || SearchServiceConfig.getInstance().getQMQRecordDomesticRequestPlatform().size() == 0
                        || (!SearchServiceConfig.getInstance().getQMQRecordDomesticRequestPlatform().contains(platform)
                        && !SearchServiceConfig.getInstance().getQMQRecordDomesticRequestPlatform().contains("all"));

                if (doNotSendAppId && doNotSendPlatform) {
                    return;
                }
            } else {
                return;
            }
            String requeststr = JsonUtil.stringify(request);

            Map<String, String> messagePropertyDic = new HashMap<>();
            messagePropertyDic.put("version", "0.0.1");
            messagePropertyDic.put("tracelogid", request.getPublicSearchParameter().getLogId() + "_rq");
            messagePropertyDic.put("request", requeststr);

            QMQCommon.setMessage(QMQConst.EsLongWordQMQTopic, appid, messagePropertyDic);
            LocalContext.getCurrentContext().trySetStrValue(ContextStrKey.DOMESTIC_SEARCH_REQUEST, requeststr);
        } catch (Exception ignored) {
        }
    }

    /**
     * @param request
     * @param response
     * @param soaVersion
     * @param tags
     */
    public void collectFieldsForCat(HotelCalendarSearchRequest request, HotelCalendarSearchResponse response
            , int soaVersion, Map<String, String> tags) {
        // ????????????
        if (!SearchServiceConfig.getInstance().getLogCatForHotelCalendarSearch()) {
            return;
        }
        if (request == null || request.getPublicSearchParameter() == null) {
            return;
        }
        /* ?????????Dictionary????????????????????????????????????!!! */
        HashMap<String, String> indexTags = new HashMap<>(120);
        ;

        collectRequestFieldsForCat(request, indexTags);

        // ????????????
        indexTags.put("servicetype", "6");
        // soaversion
        indexTags.put("soaversion", String.valueOf(soaVersion));
        // Format
        indexTags.put("format", soaVersion == 2 ? CServiceUtil.getClientFormat() : "XML");

        collectResponseFiledsForCat(response, indexTags);

        if (tags != null && tags.size() > 0) {
            for (Map.Entry<String, String> tag : tags.entrySet()) {
                if (tag == null) {
                    continue;
                }
                indexTags.put(tag.getKey(), tag.getValue());
            }
        }

        SSContext.setCatIndexTags(indexTags);
    }

    private void collectRequestFieldsForCat(HotelCalendarSearchRequest request, HashMap<String, String> indexTags) {
        if (indexTags == null) {
            return;
        }

        PublicParametersEntity args = request.getPublicSearchParameter();
        // #region ??????????????????
        // ?????????????????????
        indexTags.put("starlist", args.getStarList());
        // ?????????????????????
        indexTags.put("zonelist", args.getZoneList());
        // ????????????????????????
        indexTags.put("locationlist", args.getLocationList());
        // ???????????????????????????
        indexTags.put("channellistset", args.getChannelList());
        // ?????????????????????ID??????
        int hotelListCount = StringCollectionUtils.getSplitLength(args.getHotelList());
        indexTags.put("hotellist", String.valueOf(hotelListCount));
        indexTags.put("hotelcount307", String.valueOf(hotelListCount));
        // ???????????????????????????
        indexTags.put("hotelname", StringCommonUtils.isNullOrEmpty(args.getHotelName()) ? "0" : "1");
        indexTags.put("keyword", StringCommonUtils.isNullOrWhiteSpace(args.getKeyWord()) ? "0" : "1");
        // ????????????????????????????????????
        indexTags.put("contractsceneid", String.valueOf(request.getSearchTypeEntity().getContractSceneID()));
        // ?????????????????????????????????
        int hotelcount = request.getSearchTypeEntity().getHotelCount();
        if (hotelcount < 0) {
            hotelcount = 0;
        }
        indexTags.put("hotelcount", String.valueOf(hotelcount));
        // ??????????????????????????????
        if (request.getSearchTypeEntity().getIsGetCache() != null
                && request.getSearchTypeEntity().getIsGetCache() == true) {
            indexTags.put("isgetcache", trueValue);
        } else {
            indexTags.put("isgetcache", falseValue);
        }
        // ?????????????????????
        int pageIndex = request.getSearchTypeEntity().getPageIndex();
        if (pageIndex < 0) {
            pageIndex = 0;
        }
        indexTags.put("pageindex", String.valueOf(pageIndex));
        // ????????????????????????????????????
        // ???????????????Type
        indexTags.put("searchdatatype", request.getSearchTypeEntity().getSearchDataType() == null ? SearchDataType.OffLineIntlSearch.name() : request.getSearchTypeEntity().getSearchDataType().toString());
        indexTags.put("searchtype", request.getSearchTypeEntity().getSearchType() == null ? SearchType.StandardSearch.name() : request.getSearchTypeEntity().getSearchType().toString());
        // ?????????????????????ID
        int cityId = args.getCity();
        if (cityId < 0) {
            cityId = 0;
        }
        indexTags.put("city", String.valueOf(cityId));

        // ????????????????????????
        if (args.getCheckInDate() != null) {
            indexTags.put("checkindate", DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(args.getCheckInDate()));
        }
        // ????????????????????????
        if (args.getCheckOutDate() != null) {
            indexTags.put("checkoutdate", DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(args.getCheckOutDate()));
        }
        // ?????????????????????
        indexTags.put("star", String.valueOf(args.getStar()));
        // ???????????????????????????
        indexTags.put("ordername", args.getOrderName() == null ? StringUtils.EMPTY : args.getOrderName().toString());
        // ???????????????????????????
        indexTags.put("ordertype", args.getOrderType() == null ? StringUtils.EMPTY : args.getOrderType().toString());
        // ????????????????????????????????????
        indexTags.put("usegivenchannelhotelscore", args.getUseGivenChannelHotelScore() == null ? GivenChannelHotelScore.None.name() : args.getUseGivenChannelHotelScore().toString());
        // ???????????????????????????
        indexTags.put("iscashback", args.getIsCashBack() ? trueValue : falseValue);
        // ????????????????????????????????????
        indexTags.put("showshadowrooms", args.getShowShadowRooms() ? trueValue : falseValue);
        // ????????????
        indexTags.put("breakfastnumfilter", StringCommonUtils.isNullOrWhiteSpace(args.getBreakfastNum()) ? falseValue : trueValue);
        // ?????????
        indexTags.put("fgtopp", args.getFgToPP() ? trueValue : falseValue);
        // ???????????????????????????
        indexTags.put("isrequesttravelmoney", args.getIsRequestTravelMoney() ? trueValue : falseValue);
        // ???????????????
        indexTags.put("onlyfgprice", args.getOnlyFGPrice() ? trueValue : falseValue);
        indexTags.put("onlyppprice", args.getOnlyPPPrice() ? trueValue : falseValue);
        // ??????
        indexTags.put("breakfast", args.getBreakFast() ? trueValue : falseValue);
        // ??????????????????(????????????608189)
        indexTags.put("propertyvalueidlist", (args.getPropertyValueIDList() != null) ? args.getPropertyValueIDList() : "");
        indexTags.put("propertycodes", (args.getPropertyCodeList() != null) ? args.getPropertyCodeList() : "");
        // ??????
        indexTags.put("ispromoteroomtype", (args.getIsPromoteRoomType() != null) ? args.getIsPromoteRoomType() : "");
        // ??????
        indexTags.put("room", String.valueOf(args.getRoom()));
        // ??????
        if (request.getPersonaInfoEntity() != null) {
            String scenario = request.getPersonaInfoEntity().getScenario();
            indexTags.put("scenario", scenario != null ? scenario : "");
        }
        if (args.getHotelTagsFilter() != null) {
            // ????????????
            indexTags.put("tagsfreecancelationroom", args.getHotelTagsFilter().contains(HotelTagFilterType.FreeCancelationRoom.getValue()) ? trueValue : falseValue);
            // ????????????
            indexTags.put("tagsctripchoice", args.getHotelTagsFilter().contains(HotelTagFilterType.CtripChoice.getValue()) ? trueValue : falseValue);
            // ????????????
            indexTags.put("tagsctripselfrun", args.getHotelTagsFilter().contains(HotelTagFilterType.CtripSelfRun.getValue()) ? trueValue : falseValue);
            // ????????????
            indexTags.put("tagslowpricesupplier", args.getHotelTagsFilter().contains(HotelTagFilterType.LowPriceSupplier.getValue()) ? trueValue : falseValue);
        }
        // ????????????
        indexTags.put("pricearea", !StringCommonUtils.isNullOrWhiteSpace(args.getMultiPriceSection()) ? args.getMultiPriceSection() : "");
        if (!StringCommonUtils.isNullOrWhiteSpace(args.getMultiPriceSection())) {
            args.getMultiPriceSection();
        }

        indexTags.put("sortstagedistance", String.valueOf(args.getSortStageDistance()));

        if (args.getSwitchList() == null || args.getSwitchList().size() == 0) {
            indexTags.put("switchlist", "0");
        } else {
            indexTags.put("switchlist", StringCommonUtils.join(args.getSwitchList(), ","));
        }

        if (request.getFacilityEntity() != null) {
            HotelFacilitiesEntity facility = request.getFacilityEntity();
            // ????????????
            if (facility.getLowPrice() > 0 || facility.getHighPrice() > 0) {
                indexTags.put("pricearea", facility.getLowPrice() + "," + facility.getHighPrice());
            } else {
                if (indexTags.containsKey("pricearea") && trueValue.equals(indexTags.get("pricearea"))) {
                } else {
                    indexTags.put("pricearea", "");
                }
            }
            // ??????
            indexTags.put("hotelbrandlist", facility.getHotelBrand() > 0 ? String.valueOf(facility.getHotelBrand()) : facility.getHotelBrandList());
            // ??????
            indexTags.put("hotelmgrgrouplist", (facility.getHotelMgrGroupList() != null) ? facility.getHotelMgrGroupList() : "");
            // ?????????
            indexTags.put("metro", String.valueOf(facility.getMetro()));
            // ????????????
            indexTags.put("isjustifyconfirm", StringUtils.trimToEmpty(facility.getIsJustifyConfirm()));
            // ??????
            if (facility.getIsCanReserve() != null && facility.getIsCanReserve().booleanValue() == true) {
                indexTags.put("iscanreserve", trueValue);
            } else {
                indexTags.put("iscanreserve", falseValue);
            }
        }
        // ??????????????????
        indexTags.put("userlevel", args.getUserProfileLevel());
        // #endregion
        // #region ?????????????????????
        // ????????????
        indexTags.put("cityname", null);
        // ????????????
        indexTags.put("interval", "0");
        // ??????????????????
        indexTags.put("ismapsearch", "F");
        // ?????????????????????
        indexTags.put("hitsearchlogic", "0");
        // ??????????????????????????????
        indexTags.put("dbname", "searchHotelCalendar");
        // ?????????????????????????????????????????????
        indexTags.put("responsehotelcount", "0");
        // ???????????????????????????????????????
        indexTags.put("responseallhotelcount", "0");
        // ???????????????????????????ErrorCode
        indexTags.put("errorcode", "");
        // FixSubHotel
        indexTags.put("fixsubhotel", args.getFixSubHotel() ? trueValue : falseValue);
        // ticketgiftsnum
        indexTags.put("ticketgiftsnum", "0");
        // optionalhotelnum
        indexTags.put("optionalhotelnum", "0");
        // floatingpricenum
        indexTags.put("floatingpricenum", "0");
        // ticketusercouponnum
        indexTags.put("ticketusercouponnum", "0");
        // ??????????????????CC_SPD(0:???CC_SPD,1:CC_SPD)
        indexTags.put("spider", "0");
        // ?????????
        indexTags.put("singlehotel", hotelListCount == 1 ? trueValue : falseValue);
        // ????????????
        indexTags.put("range", String.valueOf(DateCommonUtils.getDaysBetween(args.getCheckInDate().getTime(), args.getCheckOutDate().getTime())));
        // ???????????????????????????
        indexTags.put("checkinoffset", String.valueOf(DateCommonUtils.getDaysBetween(new java.util.Date(), args.getCheckInDate().getTime())));

        if (SSContext.getBusinessConfig().getEnableRecordCheckinOffsetDaysToInt()) {
            indexTags.put("checkin_offset_days", String.valueOf(DateCommonUtils.getDaysBetween(new java.util.Date(), args.getCheckInDate().getTime())));
        }


        // ????????????(ws/ws3/drws/drws3)
        indexTags.put("cluster", ConfigurationTypeConst.getCluster());
        // ????????????(ws_jq|ws_oy/shard_jq|shard_oy/router_jq/router_oy)
        indexTags.put("group", SSContext.getBusinessConfig().getServiceGroup());
        // UID
        indexTags.put("uid", (args.getUid() != null) ? args.getUid() : "");
        // LogId=vid+pageindex ???????????????????????????
        indexTags.put("tracelogid", (args.getLogId() != null) ? args.getLogId() : "");
        // excludeticketgift
        indexTags.put("excludeticketgift", args.getExcludeTicketGift() ? trueValue : falseValue);
        // args.CouponParameter.
        if (args.getCouponParameter() != null) {
            indexTags.put("couponcount", args.getCouponParameter().getCount().toString());
            indexTags.put("excludenotdisplay", args.getCouponParameter().getExcludeNotDisplay() == null || args.getCouponParameter().getExcludeNotDisplay() ? trueValue : falseValue);
        } else {
            indexTags.put("couponcount", "0");
            indexTags.put("excludenotdisplay", falseValue);
        }
        // ??????????????????????????????ID
        if (hotelListCount == 1) {
            indexTags.put("zybusedcheck01", args.getHotelList());
            indexTags.put("hotellist307", args.getHotelList());
        } else if (args.getSwitchList() != null && (args.getSwitchList().contains(100003) || args.getSwitchList().contains(100005))) {
            indexTags.put("zybusedcheck01", args.getHotelList());
            indexTags.put("hotellist307", args.getHotelList());
        } else {
            indexTags.put("zybusedcheck01", "0");
            indexTags.put("hotellist307", "0");
        }
        indexTags.put("zybusedcheck02", "T");
        indexTags.put("isjavacluster", "T");
        indexTags.put("specialtagids", args.getSpecialTagIDs() != null ? args.getSpecialTagIDs() : "");
        indexTags.put("hotelspecialtagids", args.getHotelSpecialTagIDs() != null ? args.getHotelSpecialTagIDs() : "");
        indexTags.put("ratePlanRQ", SSContext.isRatePlanRQ() ? "T" : "F");
        indexTags.put("batchid", args.getBatchID());
        indexTags.put("batchseq", String.valueOf(args.getBatchSeq()));
        indexTags.put("invoicetargettype", String.valueOf(args.getInvoiceTargetType()));
        if (!StringCommonUtils.isNullOrEmpty(args.getFilterTicketPlatformPromotionIDList())
                && !StringCommonUtils.isNullOrEmpty(args.getPlatformPromotionIDList())
                && !StringCommonUtils.isEquals(args.getFilterTicketPlatformPromotionIDList(), args.getPlatformPromotionIDList())) {
            // ???????????????????????????????????????????????????????????????????????????????????????????????????????????????
            indexTags.put("differentcouponidparams", "T");
        }
        indexTags.put("servicecode", CServiceUtil.getAppServiceCode() != null ? CServiceUtil.getAppServiceCode() : "");
        int aid = args.getAllianceID();
        if (aid < 0) {
            aid = 0;
        }
        indexTags.put("aid", String.valueOf(aid));

        if (SearchServiceConfig.getInstance().getRecordEsUpStreamAppID()) {
            indexTags.put("upstreamappid", "0");
        }
    }

    private void collectResponseFiledsForCat(HotelCalendarSearchResponse response, HashMap<String, String> indexTags) {
        if (indexTags == null) {
            return;
        }


    }

    /**
     * ??????????????????
     * http://conf.ctripcorp.com/pages/viewpage.action?pageId=809108431
     * @param indexTags
     */
    public void setTrafficTag(Map<String, String> indexTags,HttpRequestWrapper cRequest){
        if (SearchServiceConfig.getInstance().getEnableLogInfoSecRequest() && Cat.isOriginatedFromInfoSec()) {
            indexTags.put("traffictag", "info-sec");
        }
        if (SSContext.getBusinessConfig().getEnableTrafficTagForJob()) {
            if (SSContext.getBusinessConfig().getJobTrafficAppIds().contains(cRequest.clientAppId())) {
                indexTags.put("traffictag", "job-traffic");
            }
            if (!indexTags.get("spider").equals("0")) {
                indexTags.put("traffictag", "spider-traffic");
            }
            if ("".equals(indexTags.get("traffictag"))) {
                indexTags.put("traffictag", "real-traffic");
            }
        }
    }

    public void setCallSoaNotCanBook(DebugEntity debugEntity,Map<String, String> indexTags){
        if (SearchServiceConfig.getInstance().getEnableLogCallSOAInfo()
                && !StringCommonUtils.isNullOrEmpty(debugEntity.getCallSoaNotCanBookHotelCount())) {

            indexTags.put("callsoanotcanbookcount", String.valueOf(debugEntity.getCallSoaNotCanBookHotelCount()));
        }
    }


}
