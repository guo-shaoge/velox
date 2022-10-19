#pragma once

#include "velox/connectors/Connector.h"
#include <folly/MPMCQueue.h>
#include <unordered_map>
#include <iostream>

namespace facebook::velox::connector::tidb {

class TiDBDataSource;

using TiDBDataSourcePtr = std::shared_ptr<TiDBDataSource>;

// struct TiDBDataSourceManager {
//     void registerTiDBDataSource(int64_t id, std::shared_ptr<TiDBDataSource> source) {
//         std::lock_guard<std::mutex> lock(mu_);
//         data_sources_.insert({id, source});
//     }
// 
//     TiDBDataSourcePtr getTiDBDataSource(int64_t id) {
//         std::lock_guard<std::mutex> lock(mu_);
//         auto iter = data_sources_.find(id);
//         if (iter == data_sources_.end()) {
//             return nullptr;
//         } else {
//             return iter->second;
//         }
//     }
// 
//     std::unordered_map<int64_t, std::shared_ptr<TiDBDataSource>> data_sources_;
//     std::mutex mu_;
// };

// TiDBDataSourceManager& GetTiDBDataSourceManager();

class TiDBTableHandle : public ConnectorTableHandle {
    public:
        explicit TiDBTableHandle(std::string connectorId, int64_t id)
            : ConnectorTableHandle(connectorId)
            , tidbTableReaderId_(id) {};
        std::string toString() const override {
            return std::string("TiDBTableHandle test string");
        }

        int64_t tidbTableReaderId_;
};

class TiDBColumnHandle : public ColumnHandle {
    public:
        TiDBColumnHandle() = default;
};

class TiDBDataSource final : public DataSource {
    public:
        static constexpr int queue_size = 10;

        TiDBDataSource(
                const std::shared_ptr<const RowType>& outputType,
                const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
                const std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>& columnHandles)
            : DataSource()
            , queue_(std::make_unique<folly::MPMCQueue<RowVectorPtr>>(queue_size)) {
                std::cout << "InVelox log TiDBDataSource create beg" << std::endl;
                // auto tidbTableHandle = std::dynamic_pointer_cast<TiDBTableHandle>(tableHandle);
                // GetTiDBDataSourceManager().registerTiDBDataSource(tidbTableHandle->tidbTableReaderId_, shared_from_this());
                std::cout << "InVelox log TiDBDataSource create done" << std::endl;
            }

        void addSplit(std::shared_ptr<ConnectorSplit> split) override {
            // do nothing
        }

        void addDynamicFilter(
                column_index_t /*outputChannel*/,
                const std::shared_ptr<common::Filter>& /*filter*/) override {
            VELOX_NYI("Dynamic filters not supported by TiDBConnector.");
        }

        // TODO: future
        std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future) override {
            RowVectorPtr data;
            std::cout << "InVelox log TiDBDataSource::next() beg" << std::endl;
            queue_->blockingRead(data);
            std::cout << "InVelox log TiDBDataSource::next() done" << std::endl;
            return std::optional<RowVectorPtr>{data};
        }

        uint64_t getCompletedRows() override {
            return completedRows_;
        }

        uint64_t getCompletedBytes() override {
            return completedBytes_;
        }

        std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
            // TODO: Which stats do we want to expose here?
            return {};
        }

        // Called by external TiDB code.
        void enqueue(RowVectorPtr data) {
            queue_->blockingWrite(data);
        }
    private:
        std::unique_ptr<folly::MPMCQueue<RowVectorPtr>> queue_;
        size_t completedRows_{0};
        size_t completedBytes_{0};
};

class TiDBConnector final : public Connector {
    public:
        TiDBConnector(
                const std::string& id,
                std::shared_ptr<const Config> properties)
            : Connector(id, properties) {}

        std::shared_ptr<DataSource> createDataSource(
                const std::shared_ptr<const RowType>& outputType,
                const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
                const std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>& columnHandles,
                ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx) override final {
            std::cout << "InVelox log tidb createDataSource constructed done" << std::endl;
            return std::make_shared<TiDBDataSource>(outputType, tableHandle, columnHandles);
        }

  std::shared_ptr<DataSink> createDataSink(
      std::shared_ptr<const RowType> /*inputType*/,
      std::shared_ptr<
          ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      ConnectorQueryCtx* FOLLY_NONNULL /*connectorQueryCtx*/) override final {
    VELOX_NYI("TiDBConnecotr does not support data sink.");
  }
};

class TiDBConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* FOLLY_NONNULL kTiDBConnectorName{"TiDB"};

  TiDBConnectorFactory() : ConnectorFactory(kTiDBConnectorName) {}

  explicit TiDBConnectorFactory(const char* FOLLY_NONNULL connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> properties,
      folly::Executor* FOLLY_NULLABLE executor = nullptr) override {
    return std::make_shared<TiDBConnector>(id, properties);
  }
};

class TiDBConnectorSplit : public ConnectorSplit {
    public:
        explicit TiDBConnectorSplit(const std::string& connectorId): ConnectorSplit(connectorId) {};
};

} // namespace facebook::velox::connector::tidb
