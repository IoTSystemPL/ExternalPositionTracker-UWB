#include "ExternalPositionTracker.h"

#include "FilesystemHelper.h"
#include "GridManager.h"
#include "ExecQueue.h"
#include "DataManager.h"
#include "GlobalCrossingsHandler.h"
#include "ImageOperations.h"
#include "cpprest/json.h"
#include "boost/algorithm/string.hpp"

extern "C" {
#include "MQTTClient.h"
#include "MQTTClientPersistence.h"
#include "MQTTAsync.h"
}

using namespace MovStat;

class ExternalPositionTracker::MqttClientImpl
{
	mutable std::mutex mutex, parseListMutex;
	MQTTAsync client;
	std::string brokerAddress;
	ExternalPositionTracker& owner;
	bool connected = false, initOk = false;
	std::vector<GlobalPositionData> parsedList;

	void setConnected(bool value)
	{
		std::lock_guard<std::mutex> lock(mutex);
		connected = value;
	}

	void destroyClient()
	{
		std::lock_guard<std::mutex> lock(mutex);
		initOk = false;
		if (client != nullptr)
			MQTTAsync_destroy(&client);
		client = nullptr;
	}

public:
	const char* CLIENTID = "MovStatClientMQTT";
	const char* TOPIC    = "dwm/node/+/uplink/location"; // "dwm/node/+/uplink/#" - ALL the topics
	const char* PAYLOAD  = "";
	const int  QOS       = 0;
	const long TIMEOUT   = 10000L;

	bool isConnected() const
	{
		std::lock_guard<std::mutex> lock(mutex);
		return connected;
	}
	bool isinitOk() const
	{
		std::lock_guard<std::mutex> lock(mutex);
		return initOk;
	}

	static void connlost(void *context, char *cause)
	{
		Logger::warn("ExternalPositionTracker", "MQTT", "connection lost, reconnecting");
		MqttClientImpl* inst = (MqttClientImpl*)context;
		inst->setConnected(false);
		inst->reinit();
	}

	void parsePositionData(const web::json::value& f, const char* topicName, std::string arrivalTimeStr)
	{
		auto& rec = f.at("position"_us);
		std::string tempTopic(topicName);
		std::string tagFromTopic = tempTopic.substr(9, 4);
		if (rec.at("x"_us).is_double() && rec.at("y"_us).is_double() && rec.at("z"_us).is_double() && rec.at("quality"_us).is_integer())
		{
			int recX = static_cast<int>(1000 * rec.at("x"_us).as_double());
			int recY = static_cast<int>(1000 * rec.at("y"_us).as_double());
			int recZ = static_cast<int>(1000 * rec.at("z"_us).as_double());
			int recQ = static_cast<int>(rec.at("quality"_us).as_integer());
			// casting to int from double and multiplied by 1000 to get millimeters from meters 

			std::unique_lock<std::mutex> lock(parseListMutex);
			parsedList.push_back(GlobalPositionData{ tagFromTopic, std::move(arrivalTimeStr), recX, recY, recZ, recQ });
			if (parsedList.size() >= 100)
			{
				std::vector<GlobalPositionData> toAdd(std::move(parsedList)); //allows to unlock the mutex before calling ExternalPositionTracker::addPositions
				parsedList.clear();
				lock.unlock();
				owner.addPositions(std::move(toAdd), std::chrono::system_clock::now());
			}
		}
	}

	static int msgarrvd(void* context, char* topicName, int topicLen, MQTTAsync_message* message)
	{
		if (message->payloadlen > 0)
		{
			std::string msgpyld((char*)message->payload, message->payloadlen);
			std::chrono::system_clock::time_point arrivalTime = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
			auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(arrivalTime.time_since_epoch()).count() % 1000;
			auto arrivalTimeStr = TimeManager::convertTimePointToString(arrivalTime, true, "%Y-%m-%dT%H:%M:%S") + "." + Utils::toStringWithZeroPadding(ms, 3);
			try
			{
				((MqttClientImpl*)context)->parsePositionData(web::json::value::parse(utility::conversions::to_string_t(msgpyld)), topicName, std::move(arrivalTimeStr));
			}
			catch (std::exception& ex)
			{
				Logger::warn("ExternalPositionTracker::MqttClientImpl", "msgarrvd", std::string("parsePositionData call failed: ") + ex.what());
			}
		}
 		MQTTAsync_freeMessage(&message);
 		MQTTAsync_free(topicName);
 		return 1;
	}
	static void onDisconnect(void* context, MQTTAsync_successData* response)
	{
		Logger::info("ExternalPositionTracker", "MQTT", "Successful disconnection");
		((MqttClientImpl*)context)->setConnected(false);
	}
	static void onSubscribe(void* context, MQTTAsync_successData* response)
	{
		Logger::info("ExternalPositionTracker", "MQTT", "Subscribe succeeded");
		((MqttClientImpl*)context)->setConnected(true);
	}
	static void onSubscribeFailure(void* context, MQTTAsync_failureData* response)
	{
		Logger::error("ExternalPositionTracker", "MQTT", "Subscribe failed, reason code: " + std::to_string(response ? response->code : 0) + " - " + response->message);
		((MqttClientImpl*)context)->setConnected(false);
	}
	static void onConnectFailure(void* context, MQTTAsync_failureData* response)
	{
		Logger::error("ExternalPositionTracker", "MQTT", "Connect failed, reason code: " + std::to_string(response ? response->code : 0) + " - " + response->message);
		((MqttClientImpl*)context)->setConnected(false);
	}
	static void onConnect(void* context, MQTTAsync_successData* response)
	{
		MqttClientImpl* inst = (MqttClientImpl*)context;
 		MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
 		MQTTAsync_message pubmsg = MQTTAsync_message_initializer;
		Logger::info("ExternalPositionTracker", "MQTT", "Successful connection. Subscribing to topic: " + std::string(inst->TOPIC) + " for client: " + std::string(inst->CLIENTID));
 		opts.onSuccess = onSubscribe;
 		opts.onFailure = onSubscribeFailure;
 		opts.context = inst;
		int rc;
 		if ((rc = MQTTAsync_subscribe(inst->client, inst->TOPIC, inst->QOS, &opts)) != MQTTASYNC_SUCCESS)
		{
			Logger::warn("ExternalPositionTracker", "MQTT", "Failed to start subscribe, return code: " + std::to_string(rc));
			inst->reinit();
		}
	}

	int init()
	{
		MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer;
		conn_opts.keepAliveInterval = 20;
		conn_opts.cleansession = 1;
		conn_opts.context = this;
		conn_opts.onSuccess = onConnect;
		conn_opts.onFailure = onConnectFailure;
		std::lock_guard<std::mutex> lock(mutex);
		MQTTAsync_create(&client, brokerAddress.c_str(), CLIENTID, MQTTCLIENT_PERSISTENCE_NONE, nullptr);
		MQTTAsync_setCallbacks(client, this, MqttClientImpl::connlost, MqttClientImpl::msgarrvd, nullptr);
		int rc = MQTTAsync_connect(client, &conn_opts);
		initOk = rc == MQTTASYNC_SUCCESS;
		if (!initOk)
			Logger::error("ExternalPositionTracker", "Failed to start connect,", " return code: " + std::to_string(rc));
		return rc;
	}

	void reinit()
	{
		destroyClient();
		init();
	}

	MqttClientImpl(ExternalPositionTracker& owner, std::string brokerAddress) : owner(owner), brokerAddress(std::move(brokerAddress))
	{
		init();
	}
	~MqttClientImpl()
	{
		destroyClient();
	}
};

ExternalPositionTracker::ExternalPositionTracker()
	: confMngr(*ConfigurationManager::getInstance()), extConfig(confMngr.getProcessingConfig()->globalMerging.extPos), referenceNodes(extConfig.nodes),
	  relativeRect(0, 0, confMngr.getGridConfig()->gridWidth, confMngr.getGridConfig()->gridHeight),
	  minPositionQuality(extConfig.minPositionQuality), maxHistSeconds(extConfig.maxHistSeconds), logFilePath(extConfig.logPosPath),
	  sendToServer(!confMngr.getGeneralConfig()->isTestRun() && confMngr.getGeneralConfig()->getServerIntervals(ServerTypeNodes::ext_pos_servers, false) > 0),
	  lastTimeDataSent(TimeManager::getMsSinceEpoch()), lastExtObjsZonesUpdate(TimeManager::getMsSinceEpoch()),
	  minTimeInterval(extConfig.objectContactMinTime), maxTimeInterval(extConfig.objectContactMaxTime), objContactResetTime(extConfig.objContactResetTime)
{	
	if (referenceNodes.size() > 3)
	{
		std::vector<cv::Point2f> src, dst;
		for (auto refNode : referenceNodes)
		{
			src.emplace_back((float)refNode.real_x, (float)refNode.real_y);
			dst.emplace_back((float)refNode.x,      (float)refNode.y);
		}
		homography = cv::findHomography(src, dst, cv::RANSAC);
		cv::invert(homography, inverseHomography);
	}

	if (Utils::fs::isRelativePath(logFilePath))
		logFilePath = Utils::fs::join({ confMngr.getExecDirectory(), logFilePath });
	std::string srcPosPath = extConfig.srcPosPath;

	if (!extConfig.brokerAddress.empty())
		mqttClient = std::make_unique<MqttClientImpl>(*this, extConfig.brokerAddress);

	if (!srcPosPath.empty())
	{		
		if (Utils::fs::isRelativePath(srcPosPath))
			srcPosPath = Utils::fs::join({ confMngr.getExecDirectory(), srcPosPath });

		std::vector<GlobalPositionData> positions;
		for (const auto& line : Utils::fs::readAllLines(srcPosPath))
		{
			auto split = Utils::splitString(line, ';');
			positions.push_back(GlobalPositionData{ split[2], split[0], std::stoi(split[3]), std::stoi(split[4]), std::stoi(split[5]), std::stoi(split[1]) });
		}
		if (!positions.empty())
		{
			positions.push_back(GlobalPositionData{ "time_ref", positions.front().timestamp });
			addPositions(std::move(positions), std::chrono::system_clock::now());
		}
	}
	preloadedSrcPositions = !tagHistory.empty();

	auto statsFilePath = ConfigurationManager::getProcessingConfiguration()->globalMerging.extPos.statsFilePath;
	if (!statsFilePath.empty() && (!Utils::fs::fileExists(statsFilePath) || Utils::fs::getSizeBytes(statsFilePath) == 0))
	{
		std::ofstream file(statsFilePath, std::ofstream::out | std::ofstream::trunc);
		file << "time;type;id;paired[s];unpaired[s];paired id;min[mm];max[mm];avg[mm];median[mm]" << std::endl;
	}
}
ExternalPositionTracker::~ExternalPositionTracker()
{
	for (const auto& kvp : tagHistory)
		logTagStats(kvp.first, std::chrono::system_clock::now());
}

std::vector<cv::Point2f> ExternalPositionTracker::perspectiveTransform(const std::vector<cv::Point2f>& src) const
{
	if (homography.empty())
		return src;
	std::vector<cv::Point2f> dst;
	cv::perspectiveTransform(src, dst, homography);
	return dst;
}
cv::Point2f ExternalPositionTracker::perspectiveTransform(cv::Point2f src) const
{
	return perspectiveTransform(std::vector<cv::Point2f>{ src })[0];
}

float ExternalPositionTracker::transformDistanceSquared(float dist) const
{
	auto pts = perspectiveTransform({ cv::Point2f(dist, 0.f), cv::Point2f(0.f, 0.f) });
	return CalcHelper::getDistanceSquared(pts[0], pts[1]);
}

std::vector<cv::Point2f> ExternalPositionTracker::reverseTransform(const std::vector<cv::Point2f>& src) const
{
	if (inverseHomography.empty())
		return src;
	std::vector<cv::Point2f> dst;
	cv::perspectiveTransform(src, dst, inverseHomography);
	return dst;
}

void ExternalPositionTracker::addPositions(std::vector<GlobalPositionData> entries, std::chrono::system_clock::time_point rcvTp)
{
	std::unordered_map<std::string, std::vector<GlobalPositionData>> entryMap;
	for (auto& entry : entries)
		entryMap[entry.tagId].push_back(std::move(entry));
		
	auto timeOffset = std::chrono::milliseconds::zero();
	auto refTimeIt = entryMap.find("time_ref");
	if (refTimeIt != entryMap.end())
	{
		timeOffset = std::chrono::duration_cast<std::chrono::milliseconds>(rcvTp - refTimeIt->second.back().getTimepoint());
		entryMap.erase(refTimeIt);
	}
	if (!extConfig.acceptedTagIds.empty())
	{
		for (auto it = entryMap.begin(); it != entryMap.end();)
		{
			if (extConfig.acceptedTagIds.find(it->first) == extConfig.acceptedTagIds.end())
				it = entryMap.erase(it);
			else
				++it;
		}
	}
	for (auto& kvp : entryMap)
	{
		std::vector<cv::Point2f> src;
		std::transform(kvp.second.begin(), kvp.second.end(), std::back_inserter(src),
						[](auto entry) { return cv::Point2f((float)entry.coordsX, (float)entry.coordsY); });
		auto dst = perspectiveTransform(src);
		for (size_t i = 0; i < kvp.second.size(); ++i)
			kvp.second[i].relativeCoords = dst[i];

		if (timeOffset.count() != 0)
		{
			for (auto& entry : kvp.second)
			{
				entry.tp = entry.getTimepoint() + timeOffset;
				auto tpDur = entry.tp.time_since_epoch();
				entry.timestamp = TimeManager::convertTimePointToString(entry.tp, true, "%Y-%m-%dT%H:%M:%S") + "." + 
										Utils::padLeft(std::to_string((std::chrono::duration_cast<std::chrono::milliseconds>(tpDur) - 
																		std::chrono::duration_cast<std::chrono::seconds>(tpDur)).count()), 3, '0');
			}
		}
		std::lock_guard<std::mutex> lock(histMutex);
		if (extObjsZones.find(kvp.first) == extObjsZones.end())
			extObjsZones.insert({ kvp.first, decltype(extObjsZones)::mapped_type() });
		auto tagIt = tagHistory.find(kvp.first);
		for (auto& entry : kvp.second)
			addPosition(entry, tagIt);

		if (sendToServer)
			toSend[kvp.first] = TagPositionReport(kvp.second.back());
	}
}

void ExternalPositionTracker::addPosition(const GlobalPositionData& entry, decltype(tagHistory)::iterator& tagIt)
{
	if (!relativeRect.contains(entry.relativeCoords))
		return;
	
	auto tp = std::chrono::round<std::chrono::duration<long long, std::ratio<1LL, 10LL>>>(entry.getTimepoint()); //round timepoint to the 10th of a second
	if (tagIt == tagHistory.end())
	{
		auto randColor = Utils::getRandomColor();
		colorMap.insert({ entry.tagId, std::make_pair(randColor, Utils::getContrastColor(randColor)) });
		tagIt = tagHistory.insert({ entry.tagId, decltype(tagHistory)::mapped_type() }).first;
		tagPairTimes[entry.tagId] = { tp, tp };
	}
	auto histIt = tagIt->second.find(tp);
	if (histIt == tagIt->second.end())
	{
		if (tagIt->second.empty() || tp > tagIt->second.rbegin()->first)
		{
			tagIt->second.insert({ tp, entry });

			if (!logFilePath.empty())
			{
				uwbLogFile.open(logFilePath, std::ofstream::out | std::ofstream::app);
				uwbLogFile << entry.timestamp << ";" << entry.quality << ";" << entry.tagId << ";" <<
					entry.coordsX << ";" << entry.coordsY << ";" << entry.coordsZ << std::endl;
				uwbLogFile.close();
			}

			GridManager::getInstance()->incrementHeat(entry.getImageCoords(), entry.tagId);
		}
	}
	else if (entry.quality > histIt->second.quality)
		histIt->second = entry;

#ifdef SHOW_IMAGE_PROCESSES
	if (confMngr.getGeneralConfig()->enableDebugView())
		receivedTagHistory[entry.tagId][tp] = entry;
#endif
}

void ExternalPositionTracker::clearRange(const std::string& tagId, std::chrono::system_clock::time_point start, std::chrono::system_clock::time_point end)
{
	std::lock_guard<std::mutex> lock(histMutex);
	auto tagIt = tagHistory.find(tagId);
	if (tagIt != tagHistory.end())
	{
		if (start - tagPairTimes[tagId].second > std::chrono::seconds(1))
			logTagStats(tagId, start);
		tagPairTimes[tagId].first = std::max(tagPairTimes[tagId].first, start);
		tagPairTimes[tagId].second = std::max(tagPairTimes[tagId].second, end);
		tagIt->second.erase(tagIt->second.lower_bound(start), tagIt->second.upper_bound(end));
	}
}

std::map<std::string, ExtHistMap> ExternalPositionTracker::getTagHistory() const
{
	std::lock_guard<std::mutex> lock(histMutex);
	return tagHistory; //return copy for thread safety
}

void ExternalPositionTracker::drawNodes(MatW& canvas, int maxHistS) const
{
	auto drawTime = std::chrono::system_clock::now();
	auto ageLimit = std::chrono::seconds(maxHistS);

	std::lock_guard<std::mutex> lock(histMutex);
	for (const auto& tagHist : receivedTagHistory)
	{
		cv::Point2d tagCentralPosition;
		int ptsQty = 0;
		double totalWeight = 0.;
		auto colorPair = colorMap.at(tagHist.first);
		auto color = colorPair.first, contrastColor = colorPair.second;
		bool useConstColor = true;
		cv::Point prevPt;
		for (auto it = tagHist.second.rbegin(); (it != tagHist.second.rend()) && (ageLimit.count() == 0 || (drawTime - it->second.getTimepoint() < ageLimit)); ++it)
		{
			if (preloadedSrcPositions && it->second.getTimepoint() > drawTime)
				continue;

			if ((it->second.quality >= minPositionQuality) || (it->second.quality == 0)) // quality is equal to 0 when tag see only 3 anchors (mostly when tag is in static position)
			{
				cv::Point pt = it->second.getImageCoords();
				cv::circle(canvas.getIOA(), pt, 2, (useConstColor = !useConstColor) ? cv::Scalar(255, 200, 0) : color, -1);
				if (prevPt != cv::Point())
					cv::line(canvas.getIOA(), prevPt, pt, color);
				prevPt = pt;

				//calculate mass center of coordinates
				double weight = 1. / ++ptsQty;
				tagCentralPosition += weight * cv::Point2d(pt);
				totalWeight += weight;
			}
		}

		if (ptsQty > 0)
		{
			tagCentralPosition /= totalWeight;
			cv::circle(canvas.getIOA(), tagCentralPosition, 3, contrastColor, -1);
			cv::circle(canvas.getIOA(), tagCentralPosition, 2, color,         -1);
			if (extConfig.tagLabelSize > 0)
			{
				Utils::ImgOps::drawString(canvas, tagHist.first, tagCentralPosition, extConfig.tagLabelSize,
										  color, contrastColor, Utils::RectCorner::B);
			}
		}
	}
}

void ExternalPositionTracker::drawRefNodes(MatW& canvas) const
{
	for (auto refNode : referenceNodes)
	{
		cv::Point pt_real((int)(refNode.x * canvas.cols() / confMngr.getGridConfig()->gridWidth), 
			              (int)(refNode.y * canvas.rows() / confMngr.getGridConfig()->gridHeight));
		cv::circle(canvas.getIOA(), pt_real, 4, cv::Scalar(0, 255, 0), -1);
	}
}

namespace //anonymous
{
	std::chrono::milliseconds maxDiff(500);
	double findMinDistanceSq(const GlobalTrackedObject* obj, const std::vector<const GlobalTrackedObject*> objs, globalTrackedPointsMap_t::const_iterator it)
	{
		double minDistSq = -1.;
		for (const GlobalTrackedObject* second : objs)
		{
			if (second == obj)
				continue;
			auto closestSecondObjPosIt = Utils::findClosest(second->getTrackedPoints(), it->first);
			if (std::chrono::abs(it->first - closestSecondObjPosIt->first) > maxDiff ||
				!second->getExtId(closestSecondObjPosIt->first).empty()) //don't measure contacts between two externally tracked objects
			{
				continue;
			}
			double distSq = CalcHelper::getDistanceSquared(it->second.getGridPosition(), closestSecondObjPosIt->second.getGridPosition());
			if (minDistSq < 0. || distSq < minDistSq)
				minDistSq = distSq;
		}
		return minDistSq;
	}

	void updateContact(std::vector<ExternalObjectContactData>& result, globalTrackedPointsMap_t::const_iterator it, 
					   extPosHist_t::const_iterator extPosIt, double minDistSq, int zoneId,
					   float distMinLimitSquare, float distMaxLimitSquare,
					   std::chrono::milliseconds minTimeInterval, std::chrono::milliseconds maxTimeInterval)
	{
		if (!result.empty() && !result.back().isFinished() && result.back().zone != zoneId)
		{
			result.back().contactEnd = it->first;                                                                     //finish contact in previous zone
			result.push_back({ extPosIt->second.first, it->first, std::chrono::system_clock::time_point(), zoneId }); //automatically start it in the new zone
		}
		if (result.empty() || result.back().isFinished())
		{	//currently not in contact
			if (minDistSq < distMinLimitSquare)
				result.push_back({ extPosIt->second.first, it->first, std::chrono::system_clock::time_point(), zoneId });
		}
		else
		{	//contact in progress
			std::chrono::system_clock::time_point startTp = result.back().contactStart; //find contact start time including short breaks and zone changes
			for (auto rit = ++result.rbegin(); rit != result.rend(); ++rit)
			{
				if (rit->tagId != result.back().tagId || startTp - rit->contactEnd > minTimeInterval)
					break;
				startTp = rit->contactStart;
			}
			if (minDistSq > (it->first - startTp > maxTimeInterval ? distMaxLimitSquare : distMinLimitSquare) && //object further than required to maintain contact
				minDistSq < 4 * distMaxLimitSquare)																 //but close enough to consider it possibly the same one that started contact
			{
				result.back().contactEnd = it->first;
			}
		}
	}
}

std::vector<ExternalObjectContactData> ExternalPositionTracker::findExternalObjectContacts(const GlobalTrackedObject* obj, const std::vector<const GlobalTrackedObject*>& objs,
																						   float distMinLimitSquare, float distMaxLimitSquare)
{
	std::vector<ExternalObjectContactData> result;
	const auto& extPosHist = obj->getExtPosHist();
	if (extPosHist.empty())
		return result;

	const auto& trackedPoints = obj->getTrackedPoints();
	for (auto it = Utils::findClosest(trackedPoints, extPosHist.begin()->first); it != trackedPoints.end(); ++it)
	{
		auto extPosIt = Utils::findClosest(extPosHist, it->first);
		if (std::chrono::abs(it->first - extPosIt->first) > maxDiff)
		{
			if (it->first > extPosIt->first)
			{
				extPosIt = std::next(extPosIt);
				if (extPosIt == extPosHist.end())
					break; //no longer associated with external positioning object
			}
			it = trackedPoints.lower_bound(extPosIt->first);
			if (it != trackedPoints.begin())
				it = std::prev(it); //prev to account for ++it in the loop
			continue;
		}
		if (lastFinishedContactTime[extPosIt->second.first].first > it->first)
		{
			it = trackedPoints.upper_bound(lastFinishedContactTime[extPosIt->second.first].first);
			if (it != trackedPoints.begin())
				it = std::prev(it); //prev to account for ++it in the loop
			continue;
		}
		if (!result.empty() && !result.back().isFinished() && result.back().tagId != extPosIt->second.first) //contact in progress, changed associated external positioning object
			result.back().contactEnd = it->first;														     //always finish contact in case tagId is changed
		double minDistSq = findMinDistanceSq(obj, objs, it);
		if (minDistSq < 0.)
			continue; //no eligible objects, if there are not finished contacts - keep them this way
		updateContact(result, it, extPosIt, minDistSq, GridManager::getInstance()->getZoneIdAtPoint(extPosIt->second.second.getGridPosition()), 
					  distMinLimitSquare, distMaxLimitSquare, minTimeInterval, maxTimeInterval);
	}
	if (!result.empty() && !result.back().isFinished())
		result.back().contactEnd = obj->getTrackedPoints().rbegin()->first;
	return result;
}

std::vector<ExternalObjectContact> ExternalPositionTracker::extractObjectContacts(
	const std::map<std::string, std::map<std::chrono::system_clock::time_point, ExternalObjectContactData>>& data)
{
	std::vector<ExternalObjectContact> result;
	for (const auto& tagContacts : data)
	{
		auto& lastFinishedObjectContact = lastFinishedContactTime[tagContacts.first];
		std::string& customerId = lastFinishedObjectContact.second;
		std::chrono::system_clock::time_point startTp;
		for (auto it = tagContacts.second.begin(); it != tagContacts.second.end(); ++it)
		{
			auto& item = it->second;
			auto timeSincePrev = item.contactStart - lastFinishedObjectContact.first;
			lastFinishedObjectContact.first = item.contactEnd;
			if (timeSincePrev > objContactResetTime)
			{
				customerId = Utils::generateGuid();
				startTp = std::chrono::system_clock::time_point();
			}
			if (startTp == std::chrono::system_clock::time_point())
				startTp = item.contactStart;
			if (std::find(extConfig.contactExcludedZones.begin(), extConfig.contactExcludedZones.end(), item.zone) == extConfig.contactExcludedZones.end())
			{
				auto contactDuration = item.contactEnd - startTp;
				if (contactDuration > minTimeInterval)
				{
					ExternalObjectContact contact(item, customerId, startTp);
					if (!result.empty() && result.back() == contact)
						result.pop_back();
					result.emplace_back(std::move(contact));
				}
			}
		}
	}
	return result;
}

void ExternalPositionTracker::sendPositions()
{
	if (DataManager::checkSendInterval(lastTimeDataSent, ServerTypeNodes::ext_pos_servers))
	{
		std::vector<decltype(toSend)::mapped_type> toSendVec;
		{
			std::lock_guard<std::mutex> lock(histMutex);
			for (auto& kvp : toSend)
				toSendVec.push_back(std::move(kvp.second));
			toSend.clear();
		}
		ExecQueue::sendToAllServers(toSendVec, ServerTypeNodes::ext_pos_servers);
	}
}

void ExternalPositionTracker::checkCrossings()
{
	std::vector<GateCross> crosses;

	for (const auto& tagZone : extObjsZones)
	{
		if (tagZone.second.zoneId != tagZone.second.prevZoneId)
		{
			int gate;
			int dir;

			GridManager::getInstance()->getGateCross(tagZone.second.prevZoneId, tagZone.second.zoneId, gate, dir);

			if (gate != -1 && dir != -1)
			{
				GateCrossDirection crossDir;
				if (dir == 0)
					crossDir = GateCrossDirection::in;
				else if (dir == 1)
					crossDir = GateCrossDirection::out;
				else
					crossDir = GateCrossDirection::none;

				if (crossDir != GateCrossDirection::none)
				{
					//GateCross gateCross{ id, dir, frameNo, TimeManager::getMsSinceEpoch(), isFlow, obj.getId(), CameraManager::getInstance()->getLastRetrievedFrameNo() };
					GateCross gateCross{ gate, crossDir, 0, TimeManager::getMsSinceEpoch(), false, 0, 0, tagZone.first };
					crosses.push_back(gateCross);
				}   				
			}
		}
	}

	if (crosses.size() > 0)
		ExecQueue::sendToAllServers(crosses, ServerTypeNodes::crossings_servers);
}

void ExternalPositionTracker::checkTimeInZones()
{
	std::vector<ExternalObjectZoneData> zoneTimings;

	auto now = TimeManager::getMsSinceEpoch();
	if (now - lastExtObjsZonesUpdate > 1000LL)
	{
		for (auto& tagZone : extObjsZones)
		{
			auto tagHist = tagHistory.find(tagZone.first);
			if(tagHist != tagHistory.end())
			{
				int currentZone = -1;
				if (!tagHist->second.empty())
				{
					//currentZone = GridManager::getInstance()->getZoneIdAtPoint(tagHist->second.rbegin()->second.relativeCoords);
					currentZone = GridManager::getInstance()->getZoneIdAtPoint(getAvgPosition(tagZone.first, 5000, true));
					if (currentZone != tagZone.second.zoneId)
					{
						if (tagZone.second.zoneId != -1)
						{							
							//object was in legal zone and move somewherelse - we can send data
							llong intevarlMs = (now - tagZone.second.previousChangeMsEpoch);
							ExternalObjectZoneData zoneData;

							zoneData.tagId = tagZone.first;
							zoneData.zoneId = tagZone.second.zoneId;
							zoneData.timeMs = intevarlMs;

							zoneTimings.push_back(zoneData);							
						} //else object moved from illegal to legal zone -> JUST start measurement

						tagZone.second.previousChangeMsEpoch = now;
					}

					tagZone.second.prevZoneId = tagZone.second.zoneId;
					tagZone.second.zoneId = currentZone;
				}				
			}
		}

		checkCrossings();
		lastExtObjsZonesUpdate = now;
	}

	std::vector<AreaIOIntervalData> zonesData;
	for (const auto& zoneTime : zoneTimings)
	{
		AreaIOIntervalData zoneData;
		zoneData.objId = zoneTime.tagId;
		zoneData.zone = zoneTime.zoneId;
		zoneData.seconds = (long) zoneTime.timeMs / 1000;

		zonesData.push_back(zoneData);
	}

	if (!zonesData.empty())
		GlobalCrossingsHandler::getInstance()->addZonesIntervals(zonesData);
}

cv::Point2f ExternalPositionTracker::getAvgPosition(const std::string& tagId,  llong historyMs, bool useTagRelativeTime)
{
	cv::Point2f avgPosition(-1.0, -1.0);

	bool noHistory = false;
	auto tagHistIt = tagHistory.find(tagId);

	if (tagHistIt == tagHistory.end())	
		noHistory = true;	
	else if (tagHistIt->second.empty())	
		noHistory = true;

	if (noHistory)
	{		
		return avgPosition;
	}
	
	auto ageLimit = std::chrono::milliseconds(historyMs);
	auto now = std::chrono::system_clock::now();

	if (useTagRelativeTime)
		now = tagHistIt->second.rbegin()->second.getTimepoint();	

	double positionX = 0.;
	double positionY = 0.;
	int noPoints = 0;

	for (auto it = tagHistIt->second.rbegin(); (it != tagHistIt->second.rend()) && ((now - it->second.getTimepoint()) < ageLimit); ++it)
	{
		positionX += it->second.relativeCoords.x;
		positionY += it->second.relativeCoords.y;
		noPoints++;
	}

	if (noPoints)
	{
		avgPosition.x = (float)positionX / noPoints;
		avgPosition.y = (float)positionY / noPoints;
	}

	return avgPosition;
}

void ExternalPositionTracker::clearOldEntries()
{
	auto removeOlderThan = std::chrono::system_clock::now() - std::chrono::seconds(maxHistSeconds);
	std::lock_guard<std::mutex> lock(histMutex);
	auto removeFunc = [removeOlderThan](decltype(tagHistory)& map) {
		for (auto& tagHist : map)
			tagHist.second.erase(tagHist.second.begin(), tagHist.second.lower_bound(removeOlderThan));
	};
	removeFunc(tagHistory);
#ifdef SHOW_IMAGE_PROCESSES
	removeFunc(receivedTagHistory);
#endif
}

void ExternalPositionTracker::logTagStats(const std::string& tagId, std::chrono::system_clock::time_point currTime) const
{
	static std::string statsFilePath = ConfigurationManager::getProcessingConfiguration()->globalMerging.extPos.statsFilePath;
	if (!statsFilePath.empty())
	{
		auto pairIt = tagPairTimes.find(tagId);
		auto paired = std::chrono::duration_cast<std::chrono::milliseconds>(pairIt->second.second - pairIt->second.first).count();
		auto unpaired = std::chrono::duration_cast<std::chrono::milliseconds>(currTime - pairIt->second.second).count();
		if (paired > 0 && unpaired > 0)
		{
			static std::ofstream file;
			file.open(statsFilePath, std::ofstream::out | std::ofstream::app);
			file << std::fixed << std::showpoint << std::setprecision(2) << 
					TimeManager::getNowString() << ";tag;" << tagId << ";" << paired / 1000. << ";" << unpaired / 1000. << std::endl;
			file.close();
		}
	}
}

ExternalObjectContact::ExternalObjectContact(const ExternalObjectContactData& data, const std::string& customerId, std::chrono::system_clock::time_point start)
{
	this->id = 0; //fixed for server to identify
	this->occupationType = 1 << (int)RegionType::OccupationEmployee | 1 << (int)RegionType::OccupationCustomer; //both client and employee
	this->seconds = (int)std::chrono::round<std::chrono::seconds>(data.contactEnd - start).count();
	this->customerId = customerId;
	this->employeeId = data.tagId;
	this->zone = std::max(data.zone, 0);
}

bool ExternalObjectContact::operator==(const ExternalObjectContact& other) const
{
	return employeeId == other.employeeId && customerId == other.customerId && zone == other.zone;
}
