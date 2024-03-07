#pragma once

#include <map>
#include <chrono>
#include <mutex> 
#include <string>

#include "InstanceContainer.h"
#include "ConfigurationManager.h"
#include "TimeManager.h"
#include "UtilityFunctions.h"
#include "GlobalTrackedPoint.h"
#include "GlobalTrackedObject.h"
#include "RegionOccupationMeasure.h"
#include "DataManager.h"

namespace MovStat
{
	struct GlobalPositionData
	{
		std::string tagId;
		std::string timestamp;

		int coordsX;
		int coordsY;
		int coordsZ;
		int quality;

		cv::Point2f relativeCoords;
		inline cv::Point getImageCoords() const
		{
			static const float tileSizeX = ConfigurationManager::getGridConfiguration()->tileSizeX, 
				               tileSizeY = ConfigurationManager::getGridConfiguration()->tileSizeY;
			return { (int)(relativeCoords.x * tileSizeX), (int)(relativeCoords.y * tileSizeY) };
		}

		inline std::chrono::system_clock::time_point getTimepoint() const
		{
			if (tp == std::chrono::system_clock::time_point())
			{
				if (timestamp.empty())
					return std::chrono::system_clock::time_point();
				const std::string format = "%Y-%m-%dT%H:%M:%S";
				size_t pointLoc = timestamp.find_first_of('.');
				tp = TimeManager::toUTC(TimeManager::convertStringToTimePoint(timestamp.substr(0, pointLoc), format));
				if (pointLoc != std::string::npos && timestamp.size() > pointLoc)
					tp += std::chrono::milliseconds(std::stoll(Utils::padRight(timestamp.substr(pointLoc + 1, 3), 3, '0'))); //round to milliseconds
			}
			return tp;
		}

		mutable std::chrono::system_clock::time_point tp; //parsed timestamp
		bool operator==(const GlobalPositionData& other) const 
		{ 
			return	      tagId == other.tagId     && 
					  timestamp == other.timestamp &&
						coordsX == other.coordsX   && 
						coordsY == other.coordsY   && 
						coordsZ == other.coordsZ   && 
						quality == other.quality;
		}
	};

	struct TagPositionReport : public GlobalPositionData //a wrapper to change format for the server
	{
		TagPositionReport() = default;
		TagPositionReport(const GlobalPositionData& base) : GlobalPositionData(base) { }
	};
	template<> inline void logJSON<TagPositionReport>(const web::json::value& object, bool sending) { } //noop

	struct ExternalObjectContactData
	{
		std::string tagId;
		std::chrono::system_clock::time_point contactStart;
		std::chrono::system_clock::time_point contactEnd;
		int zone;
		bool isFinished() const { return contactEnd != std::chrono::system_clock::time_point(); }
	};

	struct ExternalObjectContact : public RegionOccupationSnapshot //a wrapper to change format for the server
	{
		int zone = 0;
		ExternalObjectContact() = default;
		ExternalObjectContact(const RegionOccupationSnapshot& base) : RegionOccupationSnapshot(base) { }
		ExternalObjectContact(const ExternalObjectContactData& data, const std::string& customerId, std::chrono::system_clock::time_point start);
		bool operator==(const ExternalObjectContact& other) const;
	};

	struct ExternalObjectZone
	{
		std::string tagId;
		std::chrono::system_clock::time_point previousPositionTimestamp;

		llong previousChangeMsEpoch;

		int zoneId;
		int prevZoneId;

		ExternalObjectZone() : zoneId(-1), prevZoneId(-1), previousChangeMsEpoch(0) {};
	};

	struct ExternalObjectZoneData
	{
		std::string tagId;
		llong timeMs;
		int zoneId;
		//objectType;
	};
	
	typedef std::map<std::chrono::system_clock::time_point, GlobalPositionData> ExtHistMap;
	class ExternalPositionTracker : public InstanceContainer<ExternalPositionTracker>
	{
		class MqttClientImpl;
		const ConfigurationManager& confMngr;
		const ProcessingConfiguration::GlobalMerging::ExternalPositioning& extConfig;
		std::vector<ProcessingConfiguration::GlobalMerging::ExternalPositioning::ReferenceNode> referenceNodes;
		cv::Mat homography, inverseHomography;
		cv::Rect relativeRect;
		const int minPositionQuality;
		const int maxHistSeconds;
		const std::chrono::milliseconds minTimeInterval, maxTimeInterval, objContactResetTime;
		std::map<std::string, ExtHistMap> tagHistory, receivedTagHistory;  //receivedTagHistory for debug view
		std::map<std::string, std::pair<std::chrono::system_clock::time_point, std::chrono::system_clock::time_point>> tagPairTimes; //for stats file
		std::map<std::string, std::pair<std::chrono::system_clock::time_point, std::string>> lastFinishedContactTime;
		std::map<std::string, std::pair<cv::Scalar, cv::Scalar>> colorMap;
		std::map<std::string, ExternalObjectZone> extObjsZones;
		
		bool sendToServer;
		std::map<std::string, TagPositionReport> toSend;
		llong lastTimeDataSent;
		
		llong lastExtObjsZonesUpdate;

		std::mutex parseListMutex;
		std::vector<GlobalPositionData> parseList;

		mutable std::mutex histMutex;
		std::string logFilePath;
		std::ofstream uwbLogFile;
		std::unique_ptr<MqttClientImpl> mqttClient;
		bool preloadedSrcPositions;
		void addPosition(const GlobalPositionData& entry, decltype(tagHistory)::iterator& tagHistoryIter);
		cv::Point2f getAvgPosition(const std::string& tagId, llong historyMs, bool useTagRelativeTime = false);
		void checkCrossings();
		void logTagStats(const std::string& tagId, std::chrono::system_clock::time_point currTime) const;

	public:
		ExternalPositionTracker();
		~ExternalPositionTracker();

		std::map<std::string, ExtHistMap> getTagHistory() const;		
		void addPositions(std::vector<GlobalPositionData> entries, std::chrono::system_clock::time_point rcvTp);
		void clearRange(const std::string& tagId, std::chrono::system_clock::time_point start, std::chrono::system_clock::time_point end);
		std::vector<cv::Point2f> perspectiveTransform(const std::vector<cv::Point2f>& src) const;
		cv::Point2f perspectiveTransform(cv::Point2f src) const;
		float transformDistanceSquared(float dist) const;
		std::vector<cv::Point2f> reverseTransform(const std::vector<cv::Point2f>& src) const;

		void drawNodes(MatW& canvas, int maxHistS = 3600) const;
		void drawRefNodes(MatW& canvas) const;
		std::vector<ExternalObjectContactData> findExternalObjectContacts(const GlobalTrackedObject* obj, const std::vector<const GlobalTrackedObject*>& objs,
																		  float distMinLimitSquare, float distMaxLimitSquare);
		std::vector<ExternalObjectContact> extractObjectContacts(const std::map<std::string, std::map<std::chrono::system_clock::time_point, ExternalObjectContactData>>& data);
		void sendPositions();
		void checkTimeInZones();

		void clearOldEntries();
	};
}
