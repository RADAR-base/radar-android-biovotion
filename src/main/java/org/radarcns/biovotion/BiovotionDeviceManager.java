/*
 * Copyright 2017 Uniklinik Freiburg and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.biovotion;

import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothManager;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.IBinder;
import android.support.annotation.NonNull;
//import android.widget.Toast;

import org.radarcns.android.data.DataCache;
import org.radarcns.android.data.TableDataHandler;
import org.radarcns.android.device.DeviceManager;
import org.radarcns.android.device.DeviceStatusListener;
//import org.radarcns.android.util.Boast;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import ch.hevs.biovotion.vsm.ble.scanner.VsmDiscoveryListener;
import ch.hevs.biovotion.vsm.ble.scanner.VsmScanner;
import ch.hevs.biovotion.vsm.core.VsmConnectionState;
import ch.hevs.biovotion.vsm.core.VsmDescriptor;
import ch.hevs.biovotion.vsm.core.VsmDevice;
import ch.hevs.biovotion.vsm.core.VsmDeviceListener;
import ch.hevs.biovotion.vsm.parameters.Parameter;
import ch.hevs.biovotion.vsm.parameters.ParameterController;
import ch.hevs.biovotion.vsm.parameters.ParameterListener;
import ch.hevs.biovotion.vsm.protocol.stream.StreamValue;
import ch.hevs.biovotion.vsm.protocol.stream.units.Algo1;
import ch.hevs.biovotion.vsm.protocol.stream.units.Algo2;
import ch.hevs.biovotion.vsm.protocol.stream.units.BatteryState;
import ch.hevs.biovotion.vsm.protocol.stream.units.LedCurrent;
import ch.hevs.biovotion.vsm.protocol.stream.units.RawAlgo;
import ch.hevs.biovotion.vsm.protocol.stream.units.RawBoard;
import ch.hevs.biovotion.vsm.stream.StreamController;
import ch.hevs.biovotion.vsm.stream.StreamListener;
import ch.hevs.ble.lib.core.BleService;
import ch.hevs.ble.lib.core.BleServiceObserver;
import ch.hevs.ble.lib.exceptions.BleScanException;
import ch.hevs.ble.lib.scanner.Scanner;

/** Manages scanning for a Biovotion VSM wearable and connecting to it */
public class BiovotionDeviceManager implements DeviceManager, VsmDeviceListener, VsmDiscoveryListener, StreamListener, ParameterListener {
    private static final Logger logger = LoggerFactory.getLogger(BiovotionDeviceManager.class);

    private final TableDataHandler dataHandler;
    private final Context context;
    private final DeviceStatusListener biovotionService;

    private final DataCache<MeasurementKey, BiovotionVSMBloodPulseWave> bpwTable;
    private final DataCache<MeasurementKey, BiovotionVSMSpO2> spo2Table;
    private final DataCache<MeasurementKey, BiovotionVSMHeartRate> hrTable;
    private final DataCache<MeasurementKey, BiovotionVSMHeartRateVariability> hrvTable;
    private final DataCache<MeasurementKey, BiovotionVSMRespirationRate> rrTable;
    private final DataCache<MeasurementKey, BiovotionVSMEnergy> energyTable;
    private final DataCache<MeasurementKey, BiovotionVSMTemperature> temperatureTable;
    private final DataCache<MeasurementKey, BiovotionVSMGalvanicSkinResponse> gsrTable;
    private final DataCache<MeasurementKey, BiovotionVSMAcceleration> accelerationTable;
    private final DataCache<MeasurementKey, BiovotionVSMPhotoRaw> ppgRawTable;
    private final DataCache<MeasurementKey, BiovotionVSMLedCurrent> ledCurrentTable;
    private final AvroTopic<MeasurementKey, BiovotionVSMBatteryState> batteryTopic;

    private final BiovotionDeviceStatus deviceStatus;

    private boolean isClosed;
    private boolean isConnected;
    private String deviceName;
    private Pattern[] acceptableIds;

    private VsmDevice vsmDevice;
    private StreamController vsmStreamController;
    private ParameterController vsmParameterController;
    private VsmDescriptor vsmDescriptor;
    private VsmScanner vsmScanner;
    private BluetoothAdapter vsmBluetoothAdapter;
    private BleService vsmBleService;

    private final ScheduledExecutorService executor;

    private ScheduledFuture<?> gapFuture;
    private final ByteBuffer gapRequestBuffer;
    private int gap_raw_last_cnt = -1;      // latest counter index from last GAP request
    private int gap_raw_last_idx = -1;      // start index from last GAP request
    private int gap_raw_since_last = 0;     // number of records streamed since last GAP request
    private int gap_raw_to_get = 0;         // number of records requested from last GAP request
    private int gap_raw_cnt = -1;           // current latest counter index
    private int gap_raw_num = -1;           // current total number of records in storage
    private int gap_stat = -1;              // current GAP status

    private Deque<BiovotionVSMAcceleration> gap_raw_stack_acc;
    private Deque<BiovotionVSMPhotoRaw> gap_raw_stack_ppg;
    private Deque<BiovotionVSMLedCurrent> gap_raw_stack_led_current;

    private ScheduledFuture<?> utcFuture;
    private final ByteBuffer utcBuffer;

    // Code to manage Service lifecycle.
    private boolean bleServiceConnectionIsBound;
    private final ServiceConnection bleServiceConnection = new ServiceConnection() {

        @Override
        public void onServiceConnected(ComponentName name, IBinder service) {
            vsmBleService = ((BleService.LocalBinder) service).getService();
            vsmBleService.setVerbose(false);

            // The shared BLE service is now connected. Can be used by the watch.
            vsmDevice.setBleService(vsmBleService);

            logger.info("Biovotion VSM initialize BLE service");
            if (!vsmBleService.initialize(context.getApplicationContext()))
                logger.error("Biovotion VSM unable to initialize BLE service");

            // Automatically connects to the device upon successful start-up initialization
            String id = vsmDevice.descriptor().address();
            logger.info("Biovotion VSM Connecting to {} from activity {}", vsmDevice.descriptor(), this.toString());
            vsmBleService.connect(id, VsmConstants.BLE_CONN_TIMEOUT_MS);
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {
            logger.info("Biovotion VSM BLE service disconnected");
            vsmBleService = null;
        }
    };



    public BiovotionDeviceManager(Context context, DeviceStatusListener biovotionService, String groupId, TableDataHandler handler, BiovotionTopics topics) {
        this.dataHandler = handler;
        this.bpwTable = dataHandler.getCache(topics.getBloodPulseWaveTopic());
        this.spo2Table = dataHandler.getCache(topics.getSpO2Topic());
        this.hrTable = dataHandler.getCache(topics.getHeartRateTopic());
        this.hrvTable = dataHandler.getCache(topics.getHrvTopic());
        this.rrTable = dataHandler.getCache(topics.getRrTopic());
        this.energyTable = dataHandler.getCache(topics.getEnergyTopic());
        this.temperatureTable = dataHandler.getCache(topics.getTemperatureTopic());
        this.gsrTable = dataHandler.getCache(topics.getGsrTopic());
        this.accelerationTable = dataHandler.getCache(topics.getAccelerationTopic());
        this.ppgRawTable = dataHandler.getCache(topics.getPhotoRawTopic());
        this.ledCurrentTable = dataHandler.getCache(topics.getLedCurrentTopic());
        this.batteryTopic = topics.getBatteryStateTopic();

        this.biovotionService = biovotionService;
        this.context = context;

        this.bleServiceConnectionIsBound = false;

        this.executor = Executors.newSingleThreadScheduledExecutor();

        this.gap_raw_stack_acc = new ArrayDeque<>(1024);
        this.gap_raw_stack_ppg = new ArrayDeque<>(1024);
        this.gap_raw_stack_led_current = new ArrayDeque<>(1024);

        this.gapRequestBuffer = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN);
        this.utcBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

        synchronized (this) {
            this.deviceStatus = new BiovotionDeviceStatus();
            this.deviceStatus.getId().setUserId(groupId);
            this.deviceName = null;
            this.isClosed = true;
            this.isConnected = false;
            this.acceptableIds = null;
        }
    }


    public void close() {
        executor.shutdown();

        synchronized (this) {
            if (this.isClosed) {
                return;
            }
            logger.info("Biovotion VSM Closing device {}", deviceName);
            this.isClosed = true;
        }
        if (vsmScanner != null && vsmScanner.isScanning()) vsmScanner.stopScanning();
        if (vsmDevice != null && vsmDevice.isConnected()) vsmDevice.disconnect();
        if (vsmBleService != null && vsmBleService.connectionState() == BleServiceObserver.ConnectionState.GATT_CONNECTED) vsmBleService.disconnect();

        updateStatus(DeviceStatusListener.Status.DISCONNECTED);
    }


    /*
     * DeviceManager interface
     */

    @Override
    public void start(@NonNull final Set<String> acceptableIds) {
        logger.info("Biovotion VSM searching for device.");

        // Initializes a Bluetooth adapter.
        final BluetoothManager bluetoothManager = (BluetoothManager) context.getSystemService(Context.BLUETOOTH_SERVICE);
        vsmBluetoothAdapter = bluetoothManager.getAdapter();

        // Create a VSM scanner and register to be notified when VSM devices have been found
        vsmScanner = new VsmScanner(vsmBluetoothAdapter, this);
        vsmScanner.startScanning();

        synchronized (this) {
            this.acceptableIds = Strings.containsPatterns(acceptableIds);
            this.isClosed = false;
        }

        if (gapFuture != null) {
            gapFuture.cancel(false);
        }
        if (utcFuture != null) {
            utcFuture.cancel(false);
        }

        // schedule new gap request or checking gap running status
        gapFuture = executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (isConnected) {
                    paramReadRequest(VsmConstants.PID_GAP_REQUEST_STATUS);
                }
            }
        }, VsmConstants.GAP_INTERVAL_MS, VsmConstants.GAP_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // schedule new gap request or checking gap running status
        utcFuture = executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (isConnected) {
                    // set UTC time
                    byte[] time_bytes = utcBuffer.putInt((int) (System.currentTimeMillis() / 1000d)).array();
                    final Parameter time = Parameter.fromBytes(VsmConstants.PID_UTC, time_bytes);
                    paramWriteRequest(time);
                    utcBuffer.clear();
                }
            }
        }, VsmConstants.UTC_INTERVAL_MS, VsmConstants.UTC_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized boolean isClosed() {
        return isClosed;
    }

    @Override
    public BiovotionDeviceStatus getState() {
        return deviceStatus;
    }

    @Override
    public synchronized String getName() {
        return deviceName;
    }

    @Override
    public synchronized boolean equals(Object other) {
        return other == this
                || other != null && getClass().equals(other.getClass())
                && deviceStatus.getId().getSourceId() != null
                && deviceStatus.getId().equals(((BiovotionDeviceManager) other).deviceStatus.getId());
    }

    @Override
    public int hashCode() {
        return deviceStatus.getId().hashCode();
    }

    private synchronized void updateStatus(DeviceStatusListener.Status status) {
        if (status == deviceStatus.getStatus()) return;
        this.deviceStatus.setStatus(status);

        if (status == DeviceStatusListener.Status.DISCONNECTED && bleServiceConnectionIsBound) {
            context.unbindService(bleServiceConnection);
            bleServiceConnectionIsBound = false;
            logger.info("Biovotion VSM BLE service unbound.");
        }

        if (status == DeviceStatusListener.Status.DISCONNECTED) {
            logger.info("Biovotion VSM empty remaining data stacks.");
            // empty remaining records from raw data stacks
            while (!gap_raw_stack_acc.isEmpty()) {
                dataHandler.addMeasurement(accelerationTable, deviceStatus.getId(), gap_raw_stack_acc.removeFirst());
                dataHandler.addMeasurement(ppgRawTable, deviceStatus.getId(), gap_raw_stack_ppg.removeFirst());
            }
            while (!gap_raw_stack_led_current.isEmpty()) {
                dataHandler.addMeasurement(ledCurrentTable, deviceStatus.getId(), gap_raw_stack_led_current.removeFirst());
            }
        }

        this.biovotionService.deviceStatusUpdated(this, status);
    }


    /*
     * VsmDeviceListener interface
     */

    @Override
    public void onVsmDeviceConnected(@NonNull VsmDevice device, boolean ready) {
        if (!ready)
            return;

        logger.info("Biovotion VSM device connected.");

        updateStatus(DeviceStatusListener.Status.CONNECTED);

        vsmDevice = device;

        vsmStreamController = device.streamController();
        vsmStreamController.addListener(this);
        vsmParameterController = device.parameterController();
        vsmParameterController.addListener(this);

        // set UTC time
        byte[] time_bytes = utcBuffer.putInt((int) (System.currentTimeMillis() / 1000d)).array();
        final Parameter time = Parameter.fromBytes(VsmConstants.PID_UTC, time_bytes);
        paramWriteRequest(time);
        utcBuffer.clear();

        // check for correct algo mode
        paramReadRequest(VsmConstants.PID_ALGO_MODE);

        // check for GSR mode on
        paramReadRequest(VsmConstants.PID_GSR_ON);

        this.isConnected = true;
    }

    @Override
    public void onVsmDeviceConnecting(@NonNull VsmDevice device) {
        logger.info("Biovotion VSM device connecting.");
        updateStatus(DeviceStatusListener.Status.CONNECTING);
    }

    @Override
    public void onVsmDeviceConnectionError(@NonNull VsmDevice device, VsmConnectionState errorState) {
        logger.error("Biovotion VSM device connection error: {}", errorState.toString());
        this.isConnected = false;
        updateStatus(DeviceStatusListener.Status.DISCONNECTED);

        if (vsmDevice != null) vsmDevice.removeListeners();
        if (vsmStreamController != null) vsmStreamController.removeListeners();
        vsmStreamController = null;
        if (vsmParameterController != null) vsmParameterController.removeListeners();
        vsmParameterController = null;
    }

    @Override
    public void onVsmDeviceDisconnected(@NonNull VsmDevice device, int statusCode) {
        logger.warn("Biovotion VSM device disconnected. ({})", statusCode);
        this.isConnected = false;
        updateStatus(DeviceStatusListener.Status.DISCONNECTED);

        if (vsmDevice != null) vsmDevice.removeListeners();
        if (vsmStreamController != null) vsmStreamController.removeListeners();
        vsmStreamController = null;
        if (vsmParameterController != null) vsmParameterController.removeListeners();
        vsmParameterController = null;
    }

    @Override
    public void onVsmDeviceReady(@NonNull VsmDevice device) {
        logger.info("Biovotion VSM device ready.");
        updateStatus(DeviceStatusListener.Status.READY);
        device.connect(5000);
    }


    /*
     * VsmDiscoveryListener interface
     */

    @Override
    public void onVsmDeviceFound(@NonNull Scanner scanner, @NonNull VsmDescriptor descriptor) {
        logger.info("Biovotion VSM device found.");
        vsmScanner.stopScanning();

        if (acceptableIds.length > 0
                && !Strings.findAny(acceptableIds, descriptor.name())
                && !Strings.findAny(acceptableIds, descriptor.address())) {
            logger.info("Biovotion VSM Device {} with ID {} is not listed in acceptable device IDs", descriptor.name(), "");
            biovotionService.deviceFailedToConnect(descriptor.name());
            return;
        }

        synchronized (this) {
            this.deviceName = descriptor.name();
            this.deviceStatus.getId().setSourceId(descriptor.address());
        }
        logger.info("Biovotion VSM device Name: {} ID: {}", this.deviceName, descriptor.address());

        vsmDevice = VsmDevice.sharedInstance();
        vsmDevice.setDescriptor(descriptor);

        // Bind the shared BLE service
        Intent gattServiceIntent = new Intent(context, BleService.class);
        bleServiceConnectionIsBound = context.bindService(gattServiceIntent, bleServiceConnection, context.BIND_AUTO_CREATE);

        vsmDevice.addListener(this);
    }

    @Override
    public void onScanStopped(@NonNull Scanner scanner) {
        logger.info("Biovotion VSM device scan stopped.");
    }

    @Override
    public void onScanError(@NonNull Scanner scanner, @NonNull BleScanException throwable) {
        logger.error("Biovotion VSM Scanning error. Code: "+ throwable.getReason());
        // TODO: handle error
    }



    /*
     * ParameterListener interface
     */

    @Override
    public void onParameterWritten(@NonNull final ParameterController ctrl, int id) {
        logger.info("Biovotion VSM Parameter written: {}", id);

        if (id == VsmConstants.PID_UTC) paramReadRequest(VsmConstants.PID_UTC);
    }

    @Override
    public void onParameterWriteError(@NonNull final ParameterController ctrl, int id, final int errorCode) {
        logger.error("Biovotion VSM Parameter write error, id={}, error={}", id, errorCode);
    }

    @Override
    public void onParameterRead(@NonNull final ParameterController ctrl, @NonNull Parameter p) {
        logger.info("Biovotion VSM Parameter read: {}", p);

        // read algo_mode parameter; if not mixed algo/raw, set it to that
        if (p.id() == VsmConstants.PID_ALGO_MODE) {
            if (p.value()[0] != VsmConstants.MOD_MIXED_VITAL_RAW) {
                // Set the device into mixed (algo + raw) mode. THIS WILL REBOOT THE DEVICE!
                logger.warn("Biovotion VSM setting algo mode to MIXED_VITAL_RAW. The device will reboot!");
                final Parameter algo_mode = Parameter.fromBytes(VsmConstants.PID_ALGO_MODE, new byte[] {(byte) VsmConstants.MOD_MIXED_VITAL_RAW});
                paramWriteRequest(algo_mode);
                //Boast.makeText(context, "Rebooting Biovotion device (switch to mixed algo mode)", Toast.LENGTH_LONG).show();
            }
        }

        // read gsr_on parameter; if not on, activate
        else if (p.id() == VsmConstants.PID_GSR_ON) {
            if (p.value()[0] != 0x01) {
                // Turn on GSR module
                logger.info("Biovotion VSM activating GSR module");
                final Parameter gsr_on = Parameter.fromBytes(VsmConstants.PID_GSR_ON, new byte[] {(byte) 0x01});
                paramWriteRequest(gsr_on);
            }
        }

        // read device UTC time
        else if (p.id() == VsmConstants.PID_UTC) {
            logger.info("Biovotion VSM device Unix timestamp is: {}", p.valueAsInteger());
            //Boast.makeText(context, "Biovotion device UTC: " + p.valueAsInteger(), Toast.LENGTH_LONG).show();
        }

        /**
         * GAP request chain: gap status -> num data -> latest counter -> calculate gap -> gap request
         *
         * GAP requests will trigger streaming of data saved on the device, either raw data (which is never automatically streamed),
         * or algo data (which was not streamed due to missing connection).
         *
         * A GAP request will need the following parameters:
         * - gap_type: the type of data to be streamed
         * - gap_start: the counter value from where to begin streaming
         * - gap_range: the range, i.e. number of samples to be streamed
         *
         * Data is saved internally in a RingBuffer like structure, with two public values as an interface:
         * - number of data in the buffer (NUMBER_OF_[]_SETS_IN_STORAGE)
         * - latest counter value (LAST_[]_COUNTER_VALUE)
         * The first will max out once the storage limit is reached, and new values will overwrite old ones in a FIFO behaviour.
         * The second will keep incrementing when new samples are recorded into storage. (4 Byte unsigned int, overflow after ~2.66 years @ 51.2 Hz)
         *
         * The newest data sample has the id (counter value) LAST_[]_COUNTER_VALUE, the oldest has the id 0
         * A GAP request will accordingly stream 'backwards' w.r.t. sample id's and time:
         * Suppose gap_start = 999 and gap_range = 1000, the oldest 1000 samples will be streamed, in reverse-chronological order. (caveat see below 'pages')
         *
         * In the case of this application, the most recent raw data is requested at a fixed rate, in the following fashion:
         * - the current GAP status is queried, if a GAP request is running (=0) the new request is aborted
         * - the number of records and latest counter value are queried
         * - the new starting index is the latest counter value
         * - the range of values to request is calculated via the difference of the current latest counter and the latest counter at time of the last GAP request
         * - a new GAP request is issued with the calculated values, and the current latest counter value is saved
         *
         * Pages:
         * The data on the device is stored in pages of a specific number of samples (e.g. 17 in case of raw data). When getting data via GAP request,
         * data will always be streamed in whole pages, i.e. if the start or end point of a GAP request is in the middle of a page, that whole page will
         * still be streamed. This may result in more data being streamed than was requested.
         *
         */

        // read GAP request status
        else if (p.id() == VsmConstants.PID_GAP_REQUEST_STATUS) {
            gap_stat = p.value()[0];
            if (gap_stat == 0) {
                logger.info("Biovotion VSM GAP request running");
                return;
            }
            // TODO: handle GAP error statuses (gap_stat > 1)

            logger.debug("Biovotion VSM GAP status: raw_cnt:{} | raw_num:{} | gap_stat:{}", gap_raw_cnt, gap_raw_num, gap_stat);

            paramReadRequest(VsmConstants.PID_NUMBER_OF_RAW_DATA_SETS_IN_STORAGE);
        }

        // read number of raw data sets currently in device storage
        else if (p.id() == VsmConstants.PID_NUMBER_OF_RAW_DATA_SETS_IN_STORAGE) {
            gap_raw_num = p.valueAsInteger();
            logger.debug("Biovotion VSM GAP status: raw_cnt:{} | raw_num:{} | gap_stat:{}", gap_raw_cnt, gap_raw_num, gap_stat);

            paramReadRequest(VsmConstants.PID_LAST_RAW_COUNTER_VALUE);
        }

        // read current latest record counter id. may initiate GAP request here
        else if (p.id() == VsmConstants.PID_LAST_RAW_COUNTER_VALUE) {
            gap_raw_cnt = p.valueAsInteger();
            if (gap_raw_last_cnt < 0) gap_raw_last_cnt = gap_raw_cnt; // init

            int new_idx = gap_raw_cnt;

            // set the number of records to request at this point
            //int records_to_get = VsmConstants.GAP_NUM_PAGES * VsmConstants.GAP_MAX_PER_PAGE_VITAL_RAW;
            int records_to_get = gap_raw_cnt - gap_raw_last_cnt;

            logger.debug("Biovotion VSM GAP status: raw_cnt:{}:{} | raw_toget:{}:{} | gap_stat:{}", gap_raw_cnt, gap_raw_last_cnt, records_to_get, gap_raw_to_get, gap_stat);

            // GAP request here
            if (gap_stat != 0 && gap_raw_cnt > 0 && gap_raw_num > 0
                    && records_to_get > 0
                    && (gap_raw_num - records_to_get) > 0 // cant request more records than are stored on the device
                    && (new_idx+1) % VsmConstants.GAP_MAX_PER_PAGE_VITAL_RAW == 0 // dont make a request if the index is not a multiple of the page size, to avoid getting more records than were requested
                    && records_to_get % VsmConstants.GAP_MAX_PER_PAGE_VITAL_RAW == 0) // dont make a request if the number of records to get is not a multiple of the page size, to avoid getting more records than were requested
            {
                if (gap_raw_since_last != gap_raw_to_get)
                    logger.warn("Biovotion VSM GAP num samples since last request ({}) is not equal with num records to get ({})!", gap_raw_since_last, gap_raw_to_get);

                if (!gapRequest(VsmConstants.GAP_TYPE_VITAL_RAW, new_idx, records_to_get)) {
                    logger.error("Biovotion VSM GAP request failed!");
                } else {
                    gap_raw_since_last = 0;
                    gap_raw_last_idx = new_idx;
                    gap_raw_last_cnt = gap_raw_cnt;
                    gap_raw_to_get = records_to_get;
                    paramReadRequest(VsmConstants.PID_GAP_REQUEST_STATUS);
                }
            }
            else if (gap_stat != 0 && gap_raw_cnt > 0 && gap_raw_num > 0
                    && records_to_get > 0
                    && (gap_raw_num - records_to_get) > 0
                    && ((new_idx+1) % VsmConstants.GAP_MAX_PER_PAGE_VITAL_RAW != 0 || records_to_get % VsmConstants.GAP_MAX_PER_PAGE_VITAL_RAW != 0))
            {
                paramReadRequest(VsmConstants.PID_LAST_RAW_COUNTER_VALUE); // immediately try again if aborted due to page size checks
            }
        }

    }

    @Override
    public void onParameterReadError(@NonNull final ParameterController ctrl, int id) {
        logger.error("Biovotion VSM Parameter read error, id={}", id);
    }


    /**
     * make a new GAP request
     * @param gap_type data type of GAP request
     * @param gap_start counter value from where to begin (backwards, e.g. last counter)
     * @param gap_range number of records to get
     * @return write request success
     */
    public boolean gapRequest(int gap_type, int gap_start, int gap_range) {
        if (gap_start <= 0 || gap_range <= 0 || gap_range > gap_start+1) return false;

        // build the complete request value byte array
        byte[] ba_gap_req_value = gapRequestBuffer
                .put((byte) gap_type)
                .putInt(gap_start)
                .putInt(gap_range)
                .array();
        gapRequestBuffer.clear();

        // send the request
        logger.info("Biovotion VSM GAP new request: type:{} start:{} range:{}", gap_type, gap_start, gap_range);
        final Parameter gap_req = Parameter.fromBytes(VsmConstants.PID_GAP_RECORD_REQUEST, ba_gap_req_value);
        return paramWriteRequest(gap_req);
    }

    public boolean paramReadRequest(int id) {
        boolean success = vsmParameterController.readRequest(id);
        if (!success) logger.error("Biovotion VSM parameter read request error. id:{}", id);
        return success;
    }
    public boolean paramWriteRequest(Parameter param) {
        boolean success = vsmParameterController.writeRequest(param);
        if (!success) logger.error("Biovotion VSM parameter write request error. parameter:{}", param);
        return success;
    }



    /*
     * StreamListener interface
     */

    @Override
    public void onStreamValueReceived(@NonNull final StreamValue unit) {
        logger.info("Biovotion VSM Data recieved: {}", unit.type);

        switch (unit.type) {
            case BatteryState:
                BatteryState battery = (BatteryState) unit.unit;
                logger.info("Biovotion VSM battery state: cap:{} rate:{} voltage:{} state:{}", battery.capacity, battery.chargeRate, battery.voltage/10.0f, battery.state);
                deviceStatus.setBattery(battery.capacity, battery.chargeRate, battery.voltage, battery.state);
                float[] latestBattery = deviceStatus.getBattery();

                BiovotionVSMBatteryState value = new BiovotionVSMBatteryState((double) unit.timestamp, System.currentTimeMillis() / 1000d,
                        latestBattery[0], latestBattery[1], latestBattery[2], latestBattery[3]);

                dataHandler.trySend(batteryTopic, 0L, deviceStatus.getId(), value);
                break;

            case Algo1:
                Algo1 algo1 = (Algo1) unit.unit;
                deviceStatus.setBloodPulseWave(algo1.bloodPulseWave, 0);
                deviceStatus.setSpo2(algo1.spO2, algo1.spO2Quality);
                deviceStatus.setHeartRate(algo1.hr, algo1.hrQuality);
                float[] latestBPW = deviceStatus.getBloodPulseWave();
                float[] latestSpo2 = deviceStatus.getSpO2();
                float[] latestHr = deviceStatus.getHeartRateAll();

                BiovotionVSMBloodPulseWave bpwValue = new BiovotionVSMBloodPulseWave((double) unit.timestamp, System.currentTimeMillis() / 1000d,
                        latestBPW[0], latestBPW[1]);
                BiovotionVSMSpO2 spo2Value = new BiovotionVSMSpO2((double) unit.timestamp, System.currentTimeMillis() / 1000d,
                        latestSpo2[0], latestSpo2[1]);
                BiovotionVSMHeartRate hrValue = new BiovotionVSMHeartRate((double) unit.timestamp, System.currentTimeMillis() / 1000d,
                        latestHr[0], latestHr[1]);

                dataHandler.addMeasurement(bpwTable, deviceStatus.getId(), bpwValue);
                dataHandler.addMeasurement(spo2Table, deviceStatus.getId(), spo2Value);
                dataHandler.addMeasurement(hrTable, deviceStatus.getId(), hrValue);
                break;

            case Algo2:
                Algo2 algo2 = (Algo2) unit.unit;
                deviceStatus.setHeartRateVariability(algo2.hrv, algo2.hrvQuality);
                deviceStatus.setRespirationRate(algo2.respirationRate, algo2.respirationRateQuality);
                deviceStatus.setEnergy(algo2.energy, algo2.energyQuality);
                float[] latestHRV = deviceStatus.getHeartRateVariability();
                float[] latestRR = deviceStatus.getRespirationRate();
                float[] latestEnergy = deviceStatus.getEnergy();

                BiovotionVSMHeartRateVariability hrvValue = new BiovotionVSMHeartRateVariability((double) unit.timestamp, System.currentTimeMillis() / 1000d,
                        latestHRV[0], latestHRV[1]);
                BiovotionVSMRespirationRate rrValue = new BiovotionVSMRespirationRate((double) unit.timestamp, System.currentTimeMillis() / 1000d,
                        latestRR[0], latestRR[1]);
                BiovotionVSMEnergy energyValue = new BiovotionVSMEnergy((double) unit.timestamp, System.currentTimeMillis() / 1000d,
                        latestEnergy[0], latestEnergy[1]);

                dataHandler.addMeasurement(hrvTable, deviceStatus.getId(), hrvValue);
                dataHandler.addMeasurement(rrTable, deviceStatus.getId(), rrValue);
                dataHandler.addMeasurement(energyTable, deviceStatus.getId(), energyValue);
                break;

            case RawBoard:
                RawBoard rawboard = (RawBoard) unit.unit;
                deviceStatus.setTemperature(rawboard.objectTemp, rawboard.localTemp, rawboard.barometerTemp);
                deviceStatus.setGalvanicSkinResponse(rawboard.gsrAmplitude, rawboard.gsrPhase);
                float[] latestTemp = deviceStatus.getTemperatureAll();
                float[] latestGSR = deviceStatus.getGalvanicSkinResponse();

                BiovotionVSMTemperature tempValue = new BiovotionVSMTemperature((double) unit.timestamp, System.currentTimeMillis() / 1000d,
                        latestTemp[0], latestTemp[1], latestTemp[2]);
                BiovotionVSMGalvanicSkinResponse gsrValue = new BiovotionVSMGalvanicSkinResponse((double) unit.timestamp, System.currentTimeMillis() / 1000d,
                        latestGSR[0], latestGSR[1]);

                dataHandler.addMeasurement(temperatureTable, deviceStatus.getId(), tempValue);
                dataHandler.addMeasurement(gsrTable, deviceStatus.getId(), gsrValue);
                break;

            case RawAlgo:
                RawAlgo rawalgo = (RawAlgo) unit.unit;
                for (RawAlgo.RawAlgoUnit i : rawalgo.units) {
                    //logger.info("Biovotion VSM RawAlgo: red:{} | green:{} | ir:{} | dark:{}", i.red, i.green, i.ir, i.dark);
                    //logger.info("Biovotion VSM RawAlgo: x:{} | y:{} | z:{} | @:{}", i.x, i.y, i.z, (double) unit.timestamp);
                    deviceStatus.setAcceleration(i.x, i.y, i.z);
                    deviceStatus.setPhotoRaw(i.red, i.green, i.ir, i.dark);
                    float[] latestAcc = deviceStatus.getAcceleration();
                    float[] latestPPG = deviceStatus.getPhotoRaw();

                    BiovotionVSMAcceleration accValue = new BiovotionVSMAcceleration((double) unit.timestamp, System.currentTimeMillis() / 1000d,
                            latestAcc[0], latestAcc[1], latestAcc[2]);
                    BiovotionVSMPhotoRaw ppgValue = new BiovotionVSMPhotoRaw((double) unit.timestamp, System.currentTimeMillis() / 1000d,
                            latestPPG[0], latestPPG[1], latestPPG[2], latestPPG[3]);

                    // add measurements to a stack as long as new measurements are older. if a newer measurement is added, empty the stack into accelerationTable and start over
                    // since acc and ppg raw data always arrive in the same unit, can just check for one and empty both
                    if (gap_raw_stack_acc.peekFirst() != null && accValue.getTime() > gap_raw_stack_acc.peekFirst().getTime()) {
                        while (!gap_raw_stack_acc.isEmpty()) {
                            dataHandler.addMeasurement(accelerationTable, deviceStatus.getId(), gap_raw_stack_acc.removeFirst());
                            dataHandler.addMeasurement(ppgRawTable, deviceStatus.getId(), gap_raw_stack_ppg.removeFirst());
                        }
                    }
                    gap_raw_stack_acc.addFirst(accValue);
                    gap_raw_stack_ppg.addFirst(ppgValue);

                    gap_raw_since_last++;
                }
                break;

            case LedCurrent:
                LedCurrent ledcurrent = (LedCurrent) unit.unit;
                //logger.info("Biovotion VSM LedCurrent: red:{} | green:{} | ir:{} | offset:{}", ledcurrent.red, ledcurrent.green, ledcurrent.ir, ledcurrent.offset);
                deviceStatus.setLedCurrent(ledcurrent.red, ledcurrent.green, ledcurrent.ir, ledcurrent.offset);
                float[] latestLedCurrent = deviceStatus.getLedCurrent();

                BiovotionVSMLedCurrent ledValue = new BiovotionVSMLedCurrent((double) unit.timestamp, System.currentTimeMillis() / 1000d,
                    latestLedCurrent[0], latestLedCurrent[1], latestLedCurrent[2], latestLedCurrent[3]);

                // add measurements to a stack as long as new measurements are older. if a newer measurement is added, empty the stack into ledCurrentTable and start over
                if (gap_raw_stack_led_current.peekFirst() != null && ledValue.getTime() > gap_raw_stack_led_current.peekFirst().getTime()) {
                    while (!gap_raw_stack_led_current.isEmpty()) {
                        dataHandler.addMeasurement(ledCurrentTable, deviceStatus.getId(), gap_raw_stack_led_current.removeFirst());
                    }
                }
                gap_raw_stack_led_current.addFirst(ledValue);

                break;
        }
    }
}
