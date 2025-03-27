# RabbitMQ data sending and receiving

### Connection
```
<appSettings>
    <add key="RabbitMQHostName" value="192.168.1.165" />
    <add key="RabbitMQUserName" value="biplob" />
    <add key="RabbitMQPassword" value="123456" />
    <add key="RabbitMQPort" value="5672" />
</appSettings>
```
### Browser URL
```
http://192.168.1.165:15672
```
#### Sender
```
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using CloudPos.Model;
using CloudPos.RabbitMQ.Models;
using log4net;
using CloudPos.Service;
using CloudPos.Model.Enum;
using CloudPos.Model.Common;

namespace CloudPos.RabbitMQ
{
    public class RabbitMQSender
    {
        private static readonly ILog Log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        static string hostName = ConfigurationManager.AppSettings["RabbitMQHostName"];
        static string userName = ConfigurationManager.AppSettings["RabbitMQUserName"];
        static string password = ConfigurationManager.AppSettings["RabbitMQPassword"];
        static int port = Convert.ToInt32(ConfigurationManager.AppSettings["RabbitMQPort"]);

        static ConnectionFactory factory = new ConnectionFactory() { HostName = hostName, UserName = userName, Password = password, Port = port, RequestedHeartbeat = 30, AutomaticRecoveryEnabled = true, NetworkRecoveryInterval = TimeSpan.FromSeconds(10) };
        static IConnection connection;
        static IModel channel;

        CommonService commonService = new CommonService();
        #region ########## common method ##########
        private void Connect()
        {
            if (connection != null && connection.IsOpen && channel != null && channel.IsOpen)
            {
                //no need to create connection
            }
            else
            {
                connection = factory.CreateConnection();
                channel = connection.CreateModel();
            }
        }

        private async Task SendMessageAsync<T>(string taskName, T data, string dataType)
        {
            taskName = taskName + "_queue";
            bool messageSent = false;
            int maxRetries = 1;

            for (int attempt = 0; attempt <= maxRetries && !messageSent; attempt++)
            {
                try
                {
                    if (attempt > 0) await Task.Delay(1000); // Optional delay for retry

                    Connect();
                    PublishMessage(taskName, data, dataType);
                    messageSent = true;
                }
                catch (Exception ex)
                {
                    if (attempt == maxRetries)
                    {
                        LogMessageBrokerError(taskName, data, dataType, ex.Message);
                    }
                }
            }
        }
        private void PublishMessage<T>(string taskName, T data, string dataType)
        {
            var body = JsonSerializeObjectToByte(data, dataType);
            channel.QueueDeclare(queue: taskName, durable: true, exclusive: false, autoDelete: false, arguments: null);

            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;
            channel.BasicPublish(exchange: "", routingKey: taskName, basicProperties: properties, body: body);
        }
        private void LogMessageBrokerError<T>(string taskName, T data, string dataType, string exMessage)
        {
            var dataBody = JsonSerializeObjectToString(data, dataType);
            try
            {
                var messageBrokerError = new MessageBrokerError
                {
                    MESSAGE_BROKER_NAME = MessageBrokerType.RabbitMQ.ToString(),
                    EVENT_TYPE = MessageBrokerEventType.Producer.ToString(),
                    TASK_NAME = taskName,
                    DATA = dataBody,
                    ERROR_MESSAGE = exMessage,
                    ENTRY_DATE = DateTime.Now,
                };

                if (!commonService.SaveMessageBrokerLog(messageBrokerError))
                {
                    string message = "Sending failed RabbitMQ TaskName" + taskName + " error:" + exMessage + " body:" + dataBody;
                    Log.Debug(message);
                }
            }
            catch (Exception ex2)
            {
                string message = "Sending failed RabbitMQ TaskName" + taskName + " error:" + exMessage + " error2 " + ex2.Message + " body:" + dataBody;
                Log.Debug(message);
            }
        }

        private string JsonSerializeObjectToString<T>(T data, string dataType)
        {
            try
            {
                RabitMQMessageData customerMessageData = new RabitMQMessageData();
                customerMessageData.DataType = dataType;
                customerMessageData.Data = data;
                return Newtonsoft.Json.JsonConvert.SerializeObject(customerMessageData);
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
        }
        private byte[] JsonSerializeObjectToByte<T>(T data, string dataType)
        {
            RabitMQMessageData customerMessageData = new RabitMQMessageData();
            customerMessageData.DataType = dataType;
            customerMessageData.Data = data;
            return Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject(customerMessageData));
        }
        #endregion

        #region ########## customer ##########
        public async Task SendCustomerMessageAsync(bool STORE_WISE_CUSTOMER_ON_SALE, IList<Customer> customerList, IList<Store> storeList)
        {
            StringBuilder body = new StringBuilder();
            if (STORE_WISE_CUSTOMER_ON_SALE)
            {
                await SendMessageAsync(RabbitMQSenderQueue.customer.ToString() + "_" + customerList[0].STORE_CODE, customerList, "Customer");
            }
            else
            {
                foreach (var store in storeList)
                {
                    await SendMessageAsync(RabbitMQSenderQueue.customer.ToString() + "_" + store.STORE_CODE, customerList, "Customer");
                }
            }
        }
        public async Task<bool> SendCustomerListMessageAsync(bool STORE_WISE_CUSTOMER_ON_SALE, string COMPANY_CODE, string username, string password)
        {
            List<Task<bool>> tasks = new List<Task<bool>>();
            var customerService = new CustomerService();
            var _storeService = new StoreService();

            IList<Store> shopList = new List<Store>();
            shopList = _storeService.GetALLStoreList(COMPANY_CODE, "CENTRALSTORE", username, password).Where(x => x.STATUS == "ACTIVE").ToList(); ;

            long customerIteration = 0;
        //  Define a label
        GetCustomers:
            var items = customerService.GetCustomerForSendingByRabbitMQ(username, password);
            if (items.Count > 0)
            {
                customerIteration++;
                int takeCount = 500;
            // Define a label
            chechDataSize:
                RabitMQMessageData customerMessageData = new RabitMQMessageData();
                customerMessageData.DataType = "Customer";
                customerMessageData.Data = items.Take(takeCount).ToList();
                var body = Newtonsoft.Json.JsonConvert.SerializeObject(customerMessageData);
                var size = Encoding.UTF8.GetBytes(body).Length;

                double n = 0;
                if (size > 40000 && takeCount == 500)
                {
                    takeCount = 50;
                    goto chechDataSize;
                }
                else if (size > 40000 && takeCount == 50)
                {
                    takeCount = 20;
                    goto chechDataSize;
                }
                else if (size > 40000 && takeCount == 20)
                {
                    takeCount = 10;
                    goto chechDataSize;
                }
                else if (size > 40000 && takeCount == 10)
                {
                    takeCount = 5;
                    goto chechDataSize;
                }
                else if (size > 40000 && takeCount == 5)
                {
                    takeCount = 2;
                }
                n = Math.Ceiling((items.Count / (double)(takeCount)));

                for (int i = 0; i < n; i++)
                {
                    var packet = items.Skip((i * takeCount)).Take(takeCount).ToList();
                    await SendCustomerMessage(STORE_WISE_CUSTOMER_ON_SALE, packet, shopList);

                    string customerCodeList = string.Join(", ", packet.Select(element => "'" + element.CUSTOMER_ID + "'"));
                    tasks.Add(Task.Run(() => customerService.ApproveCustomerForSendByRabbitMQ(customerCodeList)));

                }
                // Wait for all asynchronous operations to complete
                bool[] results = await Task.WhenAll(tasks);

                // Check if any result is false
                if (results.Any(result => !result))
                {
                    return false;
                }
                else
                {
                    goto GetCustomers;
                }
            }
            else
            {
                if (customerIteration == 0)
                {
                    throw new Exception("No customer found to send Rabbit MQ!!!!!");
                }
                else
                {
                    return true;
                }
            }
        }
        private async Task SendCustomerMessage(bool STORE_WISE_CUSTOMER_ON_SALE, IList<Customer> customerList, IList<Store> storeList)
        {
            if (STORE_WISE_CUSTOMER_ON_SALE)
            {
                await SendMessageAsync(RabbitMQSenderQueue.customer.ToString() + "_" + customerList[0].STORE_CODE, customerList, "Customer");
            }
            else
            {
                foreach (var store in storeList)
                {
                    await SendMessageAsync(RabbitMQSenderQueue.customer.ToString() + "_" + store.STORE_CODE, customerList, "Customer");
                }
            }
        }
        #endregion

        #region ########## StoreDelivery ##########
        public async Task InsertStoreDeliveryMessageAsync(StoreDelivery storeDelivery)
        {
            await SendMessageAsync("storeDelivery", storeDelivery, "insertStoreDelivery");
        }
        public async Task<bool> SendingProductToRabbitMQAsync(string company_code, string username, string password, string isParentBarcode, string barcode, DateTime date)
        {
            try
            {
                var productSearch = new ProductSearch()
                {
                    COMPANY_CODE = company_code,
                    date = date,
                    isParentBarcode = isParentBarcode,
                    BARCODE = barcode,
                    SUB_SUBCATEGORY_NAME = ""
                };
                var response = await GetProductToRabbitMQAsync(username, password, productSearch);
                return response;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public async Task SendStoreDelivery(IList<StoreDelivery> storeDeliveryList)
        {
            var delivetyToList = storeDeliveryList.Select(x => x.DELIVERY_TO).Distinct();
            foreach (var delivetyTo in delivetyToList)
            {
                var storeDelivery = storeDeliveryList.Where(s => s.DELIVERY_TO == delivetyTo);
                await SendMessageAsync(RabbitMQSenderQueue.storeDelivery.ToString() + "_" + delivetyTo, storeDelivery, "storeDelivery");
            }

        }
        #endregion

        #region ########## product ##########
        public async Task<bool> GetProductToRabbitMQAsync(string username, string password, ProductSearch productSearch)
        {
            List<Task<bool>> tasks = new List<Task<bool>>();
            var _storeService = new StoreService();
            var _productService = new ProductService();

            List<string> barcodes = new List<string>();
            IList<Store> shopList = new List<Store>();
            shopList = _storeService.GetALLStoreList(productSearch.COMPANY_CODE, "CENTRALSTORE", username, password).Where(x => x.STATUS == "ACTIVE").ToList();;

            long productIteration = 0;
            IList<Product> items = null;
        // Define a label
        GetProducts:
            items = _productService.GetProductSendByRabbitMQ(username, password, productSearch);
            if (items.Count > 0)
            {
                productIteration++;
                int takeCount = 500;
            // Define a label
            chechDataSize:
                RabitMQMessageData customerMessageData = new RabitMQMessageData();
                customerMessageData.DataType = "product";
                customerMessageData.Data = items.Take(takeCount).ToList();
                var body = Newtonsoft.Json.JsonConvert.SerializeObject(customerMessageData);
                var size = Encoding.UTF8.GetBytes(body).Length;

                double n = 0;
                if (size > 40000 && takeCount == 500)
                {
                    takeCount = 50;
                    goto chechDataSize;
                }
                else if (size > 40000 && takeCount == 50)
                {
                    takeCount = 20;
                    goto chechDataSize;
                }
                else if (size > 40000 && takeCount == 20)
                {
                    takeCount = 10;
                    goto chechDataSize;
                }
                else if (size > 40000 && takeCount == 10)
                {
                    takeCount = 5;
                    goto chechDataSize;
                }
                else if (size > 40000 && takeCount == 5)
                {
                    takeCount = 2;
                }
                n = Math.Ceiling((items.Count / (double)(takeCount)));

                for (int i = 0; i < n; i++)
                {
                    var packet = items.Skip((i * takeCount)).Take(takeCount).ToList();
                    await SendProductMessage(packet, shopList);

                    string barcodeList = string.Join(", ", packet.Select(element => "'" + element.BARCODE + "'"));
                    tasks.Add(Task.Run(() => _productService.ApproveProductForSendByRabbitMQ(username, password, productSearch.COMPANY_CODE, barcodeList)));

                }
                // Wait for all asynchronous operations to complete
                bool[] results = await Task.WhenAll(tasks);

                // Check if any result is false
                if (results.Any(result => !result))
                {
                    return false;
                }
                else
                {
                    goto GetProducts;
                }
            }
            else
            {
                if (productIteration == 0)
                {
                    throw new Exception("No data found to send Rabbit MQ!!!!!");
                }
                else
                {
                    return true;
                }
            }
        }
        private async Task SendProductMessage(IList<Product> productList, IList<Store> storeList)
        {
            foreach (var store in storeList)
            {
                await SendMessageAsync(RabbitMQSenderQueue.product.ToString() + "_" + store.STORE_CODE, productList, "product");
            }
        }
        #endregion


    }
}
```

### Consumer

```
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using CloudPos.Model;
using CloudPos.RabbitMQ.Models;
using log4net;
using RabbitMQ.Client.Events;
using CloudPos.Service;
using CloudPos.Model.Enum;
using CloudPos.Model.Common;
using System.Threading;

namespace CloudPos.RabbitMQ
{
    public class RabbitMQConsumerV2
    {

        private static readonly ILog Log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static readonly ConnectionFactory factory = new ConnectionFactory
        {
            HostName = ConfigurationManager.AppSettings["RabbitMQHostName"],
            UserName = ConfigurationManager.AppSettings["RabbitMQUserName"],
            Password = ConfigurationManager.AppSettings["RabbitMQPassword"],
            Port = Convert.ToInt32(ConfigurationManager.AppSettings["RabbitMQPort"]),
            RequestedHeartbeat = 30,
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
        };
        private static IConnection _connection;
        private static IModel _channel;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly List<string> _queues;
        private readonly StoreDeliveryService storeDeliveryService;
        private readonly SaleService saleService;
        private readonly PurchaseReceiveService purchaseReceiveService;
        private readonly StoreRequisitionService storeRequisitionService;
        private readonly PurchaseReturnService purchaseReturnService;
        private readonly StoreDMLService storeDMLService;
        private readonly InvTrackingSummaryService invTrackingSummaryService;
        private readonly GiftVoucherService giftVoucherService;
        private readonly ProductStockService productStockService;
        private readonly CommonService commonService;


        public RabbitMQConsumerV2()
        {
            _queues = new List<string> { 
                RabbitMQConsumerQueue.update_productstock_queue.ToString(),
                RabbitMQConsumerQueue.update_sale_queue.ToString(),
                RabbitMQConsumerQueue.update_gvsales_queue.ToString(),
                RabbitMQConsumerQueue.update_storereceive_queue.ToString(),
                RabbitMQConsumerQueue.update_storetransfer_queue.ToString(),
                RabbitMQConsumerQueue.update_purchasereceive_queue.ToString(),
                RabbitMQConsumerQueue.update_storerequisition_queue.ToString(),
                RabbitMQConsumerQueue.update_purchasereturn_queue.ToString(),
                RabbitMQConsumerQueue.update_storedml_queue.ToString(),
                RabbitMQConsumerQueue.update_invtrackingsummary_queue.ToString(),
                //RabbitMQConsumerQueue.update_production_queue.ToString(),
                //RabbitMQConsumerQueue.update_conversion_queue.ToString(),
            };

            _cancellationTokenSource = new CancellationTokenSource();

            // Initialize services only once
            storeDeliveryService = new StoreDeliveryService();
            saleService = new SaleService();
            purchaseReceiveService = new PurchaseReceiveService();
            storeRequisitionService = new StoreRequisitionService();
            purchaseReturnService = new PurchaseReturnService();
            storeDMLService = new StoreDMLService();
            invTrackingSummaryService = new InvTrackingSummaryService();
            giftVoucherService = new GiftVoucherService();
            productStockService = new ProductStockService();
            commonService = new CommonService();
        }

        public void Start()
        {
            Task.Run(() => ConsumeMessages(_cancellationTokenSource.Token));
        }
        public void Stop()
        {
            if (_cancellationTokenSource != null)
            {
                _cancellationTokenSource.Cancel();
            }
            CloseConnection();
        }

        private void CloseConnection()
        {
            if (_channel != null && _channel.IsOpen)
            {
                _channel.Close();
                _channel.Dispose();
            }

            if (_connection != null && _connection.IsOpen)
            {
                _connection.Close();
                _connection.Dispose();
            }

        }
        private void ConsumeMessages(CancellationToken cancellationToken)
        {
            int retryDelay = 5000; // Initial retry delay
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    ConnectAndConsume();
                    // Monitor the connection and channel status
                    while (_connection != null && _connection.IsOpen && !cancellationToken.IsCancellationRequested)
                    {
                        Task.Delay(1000, cancellationToken); // Check connection periodically
                    }
                }
                catch (Exception ex)
                {
                    Log.Error("RabbitMQ connection error: " + ex.Message);
                    retryDelay = Math.Min(retryDelay * 2, 60000); // Exponential backoff with max 60 seconds
                    Task.Delay(retryDelay, cancellationToken); // Wait before reconnecting
                }
                finally
                {
                    CloseConnection();
                }
            }
        }
        private void ConnectAndConsume()
        {
            // Establish connection and channel
            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();

            foreach (var queue in _queues)
            {
                _channel.QueueDeclare(queue: queue, durable: true, exclusive: false, autoDelete: false, arguments: null);

                var consumer = new EventingBasicConsumer(_channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    int retryCount = 0;
                    bool processedSuccessfully = false;

                    while (retryCount <= 1 && !processedSuccessfully)
                    {
                        try
                        {
                            ProcessMessage(queue, message);
                            _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                            processedSuccessfully = true;
                        }
                        catch (Exception)
                        {
                            retryCount++;
                            if (retryCount > 1)
                            {
                                _channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                            }
                        }
                    }
                };

                // Start consuming from the queue
                _channel.BasicConsume(queue: queue, noAck: false, consumer: consumer);
            }
        }

        private void ProcessMessage(string queue, string message)
        {
            try
            {
                var model = DeserializeMessage<RabitMQMessageData>(message);
                if (model == null) return;

                if (queue == RabbitMQConsumerQueue.update_productstock_queue.ToString() && model.DataType.Equals("productstock", StringComparison.OrdinalIgnoreCase))
                {
                    ProcessProductStock(model.Data, queue, message);
                }
                else if (queue == RabbitMQConsumerQueue.update_sale_queue.ToString() && model.DataType.Equals("sale", StringComparison.OrdinalIgnoreCase))
                {
                    ProcessSale(model.Data, queue, message);
                }
                else if (queue == RabbitMQConsumerQueue.update_gvsales_queue.ToString() && model.DataType.Equals("gvsale", StringComparison.OrdinalIgnoreCase))
                {
                    ProcessGvSale(model.Data, queue, message);
                }
                else if (queue == RabbitMQConsumerQueue.update_storereceive_queue.ToString() && model.DataType.Equals("storereceive", StringComparison.OrdinalIgnoreCase))
                {
                    ProcessStoreReceive(model.Data, queue, message);
                }
                else if (queue == RabbitMQConsumerQueue.update_storetransfer_queue.ToString() && model.DataType.Equals("storedelivery", StringComparison.OrdinalIgnoreCase))
                {
                    ProcessStoreTransfer(model.Data, queue, message);
                }
                else if (queue == RabbitMQConsumerQueue.update_purchasereceive_queue.ToString())
                {
                    ProcessPurchaseReceive(model.Data, queue, message);
                }
                else if (queue == RabbitMQConsumerQueue.update_storerequisition_queue.ToString())
                {
                    ProcessStoreRequisition(model.Data, queue, message);
                }
                else if (queue == RabbitMQConsumerQueue.update_purchasereturn_queue.ToString())
                {
                    ProcessPurchaseReturn(model.Data, queue, message);
                }
                else if (queue == RabbitMQConsumerQueue.update_storedml_queue.ToString())
                {
                    ProcessStoredml(model.Data, queue, message);
                }
                else if (queue == RabbitMQConsumerQueue.update_invtrackingsummary_queue.ToString())
                {
                    ProcessInvtrackingSummary(model.Data, queue, message);
                }
                else
                {
                    SaveMessageBrokerLog(queue, message, "Unknown queue or datatype: {queue}, {model.DataType}");
                }
            }
            catch (Exception ex)
            {
                SaveMessageBrokerLog(queue, message, ex.Message);
            }
        }

        #region Process Data

        private void ProcessProductStock(object data, string queue, string message)
        {
            var productStock = DeserializeList<ProductStock>(data.ToString());
            if (productStock != null)
            {
                bool result = productStockService.UpdateProductStockForRabbitMQ(productStock);
                if (!result)
                {
                    SaveMessageBrokerLog(queue, message, "RabbitMQ consumer failed.");
                }
            }
        }
        private void ProcessSale(object data, string queue, string message)
        {
            var sSummaryList = DeserializeMessage<List<SSummary>>(data.ToString());
            bool result = saleService.Insert(sSummaryList);
            if (!result)
            {
                SaveMessageBrokerLog(queue, message, "RabbitMQ consumer failed.");
            }
        }
        private void ProcessGvSale(object data, string queue, string message)
        {
            var gvSSummaryList = DeserializeMessage<List<GvSSummary>>(data.ToString());
            bool result = giftVoucherService.SaveGvSale(gvSSummaryList);
            if (!result)
            {
                SaveMessageBrokerLog(queue, message, "RabbitMQ consumer failed.");
            }
        }
        private void ProcessStoreReceive(object data, string queue, string message)
        {
            var storeDeliveryReceiveAppList = DeserializeMessage<List<StoreDeliveryReceiveApp>>(data.ToString());
            bool result = storeDeliveryService.ReceiveStoreDeliveryAtShop(storeDeliveryReceiveAppList);
            if (!result)
            {
                SaveMessageBrokerLog(queue, message, "RabbitMQ consumer failed.");
            }
        }
        private void ProcessStoreTransfer(object data, string queue, string message)
        {
            var storeDelivery = DeserializeMessage<StoreDelivery>(data.ToString());
            string CHALLAN_NO = storeDeliveryService.InsertStoreDelivery(storeDelivery);
            if (string.IsNullOrEmpty(CHALLAN_NO))
            {
                SaveMessageBrokerLog(queue, message, "RabbitMQ consumer failed.");
            }
        }
        private void ProcessPurchaseReceive(object data, string queue, string message)
        {
            var purchaseReceiveCreate = DeserializeMessage<PurchaseReceiveCreate>(data.ToString());
            bool result = purchaseReceiveService.InsertPurchaseReceiveInStore(purchaseReceiveCreate);
            if (!result)
            {
                SaveMessageBrokerLog(queue, message, "RabbitMQ consumer failed.");
            }
        }
        private void ProcessStoreRequisition(object data, string queue, string message)
        {
            var storeRequisitionList = DeserializeMessage<List<StoreRequisition>>(data.ToString());
            string requisition_No = storeRequisitionService.Insert(storeRequisitionList);
            if (string.IsNullOrEmpty(requisition_No))
            {
                SaveMessageBrokerLog(queue, message, "RabbitMQ consumer failed.");
            }
        }
        private void ProcessPurchaseReturn(object data, string queue, string message)
        {
            var purchaseReturn = DeserializeMessage<PurchaseReturn>(data.ToString());
            bool result = purchaseReturnService.InsertPurchaseReturnAtStore(purchaseReturn);
            if (!result)
            {
                SaveMessageBrokerLog(queue, message, "RabbitMQ consumer failed.");
            }
        }
        private void ProcessStoredml(object data, string queue, string message)
        {
            var StoreDMLList = DeserializeMessage<List<StoreDML>>(data.ToString());
            string ref_no = storeDMLService.Insert(StoreDMLList);
            if (string.IsNullOrEmpty(ref_no))
            {
                SaveMessageBrokerLog(queue, message, "RabbitMQ consumer failed.");
            }
        }
        private void ProcessInvtrackingSummary(object data, string queue, string message)
        {
            var InvTrackingSummaryList = DeserializeMessage<List<InvTrackingSummary>>(data.ToString());
            bool result = invTrackingSummaryService.AddInvTrackingSummaryList(InvTrackingSummaryList);
            if (!result)
            {
                SaveMessageBrokerLog(queue, message, "RabbitMQ consumer failed.");
            }
        }

        #endregion

        private void SaveMessageBrokerLog(string taskName, string dataBody, string errorMessage)
        {
            try
            {
                RabitMQMessageData model = new RabitMQMessageData();
                if (!string.IsNullOrEmpty(dataBody) && taskName == RabbitMQConsumerQueue.update_sale_queue.ToString())
                {
                    #region update_sale_queue
                    var message = dataBody;

                    model = DeserializeMessage<RabitMQMessageData>(message);

                    if (model != null && model.DataType == "sale")
                    {
                        List<SSummary> sSummaryList = new List<SSummary>();

                        sSummaryList = DeserializeMessage<List<SSummary>>(model.Data.ToString());

                        if (sSummaryList != null && sSummaryList.Count > 0)
                        {
                            try
                            {
                                bool saleResult = saleService.Insert(sSummaryList);
                                if (!saleResult)
                                {
                                    SaveMessageBrokerLogHistory(taskName, dataBody, errorMessage);
                                }
                            }
                            catch (Exception ex)
                            {
                                throw new Exception("RabbitMQ sale save failed " + ex.Message);
                            }
                        }
                        else
                        {
                            SaveMessageBrokerLogHistory(taskName, dataBody, errorMessage);
                        }
                    }
                    #endregion
                }
                else if (!string.IsNullOrEmpty(dataBody) && taskName == RabbitMQConsumerQueue.update_productstock_queue.ToString())
                {
                    #region update_productstock_queue
                    var message = dataBody;
                    model = DeserializeMessage<RabitMQMessageData>(message);
                    if (model != null && model.DataType.ToLower() == "productstock")
                    {
                        List<ProductStock> productStock = new List<ProductStock>();
                        productStock = DeserializeList<ProductStock>(model.Data.ToString());
                        if (productStock != null)
                        {
                            var productStockService = new ProductStockService();
                            try
                            {
                                bool r = productStockService.UpdateProductStockForRabbitMQ(productStock);
                                if (!r)
                                {
                                    SaveMessageBrokerLogHistory(taskName, dataBody, errorMessage);
                                }
                            }
                            catch (Exception ex)
                            {
                                throw new Exception("RabbitMQ UpdateProductStock failed " + ex.Message);
                            }
                        }
                        else
                        {
                            SaveMessageBrokerLogHistory(taskName, dataBody, errorMessage);
                        }
                    }
                    #endregion
                }
                else
                {
                    SaveMessageBrokerLogHistory(taskName, dataBody, errorMessage);
                }
            }
            catch (Exception ex2)
            {
                string message = "Consuming failed RabbitMQ TaskName" + taskName + " error:" + errorMessage + " error2 " + ex2.Message + " body:" + dataBody;
                Log.Debug(message);
            }
        }
        private void SaveMessageBrokerLogHistory(string taskName, string dataBody, string errorMessage)
        {
            try
            {
                MessageBrokerError messageBrokerError = new MessageBrokerError
                {
                    MESSAGE_BROKER_NAME = MessageBrokerType.RabbitMQ.ToString(),
                    EVENT_TYPE = MessageBrokerEventType.Consumer.ToString(),
                    TASK_NAME = taskName,
                    DATA = dataBody,
                    ERROR_MESSAGE = errorMessage,
                    ENTRY_DATE = DateTime.Now,
                };
                var result = commonService.SaveMessageBrokerLog(messageBrokerError);
                if (!result)
                {
                    string message = "Consuming failed RabbitMQ TaskName" + taskName + " error:" + errorMessage + " body:" + dataBody;
                    Log.Debug(message);
                }
            }
            catch (Exception ex2)
            {
                string message = "Consuming failed RabbitMQ TaskName" + taskName + " error:" + errorMessage + " error2 " + ex2.Message + " body:" + dataBody;
                Log.Debug(message);
            }
        }
        private T DeserializeMessage<T>(string jsonBody)
        {
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(jsonBody);
        }
        private List<T> DeserializeList<T>(string jsonBody)
        {
            return Newtonsoft.Json.JsonConvert.DeserializeObject<List<T>>(jsonBody);
        }

    }
}
```

