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
import android.util.ArrayMap;
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
import org.radarcns.android.auth.AppSource;
import org.radarcns.android.data.DataCache;
import org.radarcns.android.device.AbstractDeviceManager;
import org.radarcns.android.device.DeviceStatusListener;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.biovotion.BiovotionVsm1Acceleration;
import org.radarcns.passive.biovotion.BiovotionVsm1BatteryLevel;
import org.radarcns.passive.biovotion.BiovotionVsm1BloodPulseWave;
import org.radarcns.passive.biovotion.BiovotionVsm1Energy;
import org.radarcns.passive.biovotion.BiovotionVsm1GalvanicSkinResponse;
import org.radarcns.passive.biovotion.BiovotionVsm1HeartRate;
import org.radarcns.passive.biovotion.BiovotionVsm1HeartRateVariability;
import org.radarcns.passive.biovotion.BiovotionVsm1LedCurrent;
import org.radarcns.passive.biovotion.BiovotionVsm1OxygenSaturation;
import org.radarcns.passive.biovotion.BiovotionVsm1PpgRaw;
import org.radarcns.passive.biovotion.BiovotionVsm1RespirationRate;
import org.radarcns.passive.biovotion.BiovotionVsm1Temperature;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/** Manages scanning for a Biovotion VSM wearable and connecting to it */
public class BiovotionDeviceManager
        extends AbstractDeviceManager<BiovotionService, BiovotionDeviceStatus>
        implements VsmDeviceListener, VsmDiscoveryListener, StreamListener, ParameterListener {
    private static final Logger logger = LoggerFactory.getLogger(BiovotionDeviceManager.class);

    private final AvroTopic<ObservationKey, BiovotionVsm1BloodPulseWave> bpwTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1OxygenSaturation> spo2Topic;
    private final AvroTopic<ObservationKey, BiovotionVsm1HeartRate> hrTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1HeartRateVariability> hrvTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1RespirationRate> rrTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1Energy> energyTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1Temperature> temperatureTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1GalvanicSkinResponse> gsrTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1Acceleration> accelerationTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1PpgRaw> ppgRawTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1LedCurrent> ledCurrentTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1BatteryLevel> batteryTopic;

    private boolean isConnected;

    private VsmDevice vsmDevice;
    private StreamController vsmStreamController;
    private ParameterController vsmParameterController;
    private VsmDescriptor vsmDescriptor;
    private VsmScanner vsmScanner;
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

    private Deque<BiovotionVsm1Acceleration> gap_raw_stack_acc;
    private Deque<BiovotionVsm1PpgRaw> gap_raw_stack_ppg;
    private Deque<BiovotionVsm1LedCurrent> gap_raw_stack_led_current;

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
            if (!vsmBleService.initialize(getService()))
                logger.error("Biovotion VSM unable to initialize BLE service");

            // Automatically connects to the device upon successful start-up initialization
            try {
                String id = vsmDevice.descriptor().address();
                logger.info("Biovotion VSM Connecting to {} from activity {}", vsmDevice.descriptor(), this.toString());
                vsmBleService.connect(id, VsmConstants.BLE_CONN_TIMEOUT_MS);
            } catch (NullPointerException ex) {
                logger.error("Biovotion VSM connect failed: {}", ex);
            }
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {
            logger.info("Biovotion VSM BLE service disconnected");
            vsmBleService = null;
        }
    };
    private Pattern[] accepTopicIds;


    public BiovotionDeviceManager(BiovotionService service) {
        super(service);

        this.bpwTopic = createTopic("android_biovotion_vsm1_blood_volume_pulse",
                BiovotionVsm1BloodPulseWave.class);
        this.spo2Topic = createTopic("android_biovotion_vsm1_oxygen_saturation",
                BiovotionVsm1OxygenSaturation.class);
        this.hrTopic = createTopic("android_biovotion_vsm1_heartrate",
                BiovotionVsm1HeartRate.class);
        this.hrvTopic = createTopic("android_biovotion_vsm1_heartrate_variability",
                BiovotionVsm1HeartRateVariability.class);
        this.rrTopic = createTopic("android_biovotion_vsm1_respiration_rate",
                BiovotionVsm1RespirationRate.class);
        this.energyTopic = createTopic("android_biovotion_vsm1_energy",
                BiovotionVsm1Energy.class);
        this.temperatureTopic = createTopic("android_biovotion_vsm1_temperature",
                BiovotionVsm1Temperature.class);
        this.gsrTopic = createTopic("android_biovotion_vsm1_galvanic_skin_response",
                BiovotionVsm1GalvanicSkinResponse.class);
        this.accelerationTopic = createTopic("android_biovotion_vsm1_acceleration",
                BiovotionVsm1Acceleration.class);
        this.ppgRawTopic = createTopic("android_biovotion_vsm1_ppg_raw",
                BiovotionVsm1PpgRaw.class);
        this.ledCurrentTopic = createTopic("android_biovotion_vsm1_led_current",
                BiovotionVsm1LedCurrent.class);
        this.batteryTopic = createTopic("android_biovotion_vsm1_battery_level",
                BiovotionVsm1BatteryLevel.class);

        this.bleServiceConnectionIsBound = false;

        this.executor = Executors.newSingleThreadScheduledExecutor();

        this.gap_raw_stack_acc = new ArrayDeque<>(1024);
        this.gap_raw_stack_ppg = new ArrayDeque<>(1024);
        this.gap_raw_stack_led_current = new ArrayDeque<>(1024);

        this.gapRequestBuffer = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN);
        this.utcBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

        synchronized (this) {
            this.isConnected = false;
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed()) {
            return;
        }
        logger.info("Biovotion VSM Closing device {}", this);
        executor.shutdown();

        super.close();
    }

    public void disconnect() {
        logger.info("Biovotion VSM disconnecting.");
        this.isConnected = false;

        // stop device/ble services
        if (vsmScanner != null && vsmScanner.isScanning()) {
            vsmScanner.stopScanning();
            vsmScanner = null;
        }
        if (vsmDevice != null) {
            if (vsmDevice.isConnected()) vsmDevice.disconnect();
            vsmDevice.removeListeners();
            vsmDevice = null;
        }
        if (vsmBleService != null && vsmBleService.connectionState() == BleServiceObserver.ConnectionState.GATT_CONNECTED) {
            vsmBleService.disconnect();
            if (bleServiceConnectionIsBound) {
                getService().unbindService(bleServiceConnection);
                bleServiceConnectionIsBound = false;
            }
            vsmBleService = null;
        }

        // remove controllers
        if (vsmStreamController != null) {
            vsmStreamController.removeListeners();
            vsmStreamController = null;
        }
        if (vsmParameterController != null) {
            vsmParameterController.removeListeners();
            vsmParameterController = null;
        }

        // empty remaining raw data stacks
        while (!gap_raw_stack_acc.isEmpty()) {
            send(accelerationTopic, gap_raw_stack_acc.removeFirst());
            send(ppgRawTopic, gap_raw_stack_ppg.removeFirst());
        }
        while (!gap_raw_stack_led_current.isEmpty()) {
            send(ledCurrentTopic, gap_raw_stack_led_current.removeFirst());
        }

        logger.info("Biovotion VSM disconnected.");
    }


    /*
     * DeviceManager interface
     */

    @Override
    public void start(@NonNull final Set<String> accepTopicIds) {
        logger.info("Biovotion VSM searching for device.");

        // Initializes a Bluetooth adapter.
        BluetoothManager bluetoothManager = (BluetoothManager) getService().getSystemService(Context.BLUETOOTH_SERVICE);
        BluetoothAdapter vsmBluetoothAdapter = bluetoothManager.getAdapter();

        // Create a VSM scanner and register to be notified when VSM devices have been found
        vsmScanner = new VsmScanner(vsmBluetoothAdapter, this);
        vsmScanner.startScanning();

        updateStatus(DeviceStatusListener.Status.READY);

        synchronized (this) {
            this.accepTopicIds = Strings.containsPatterns(accepTopicIds);
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

        // schedule setting of device UTC time; this also checks if device is on a charger
        utcFuture = executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (isConnected) {
                    // set UTC time
                    byte[] time_bytes = utcBuffer.putInt((int) (System.currentTimeMillis() / 1000d)).array();
                    final Parameter time = Parameter.fromBytes(VsmConstants.PID_UTC, time_bytes);
                    paramWriteRequest(time);
                    utcBuffer.clear();
                    // check device state
                    paramReadRequest(VsmConstants.PID_DEVICE_MODE);
                }
            }
        }, VsmConstants.UTC_INTERVAL_MS, VsmConstants.UTC_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    protected synchronized void updateStatus(DeviceStatusListener.Status status) {
        if (status == DeviceStatusListener.Status.DISCONNECTED) disconnect();
        super.updateStatus(status);
    }


    /*
     * VsmDeviceListener interface
     */

    @Override
    public void onVsmDeviceConnected(@NonNull VsmDevice device, boolean ready) {
        if (!ready)
            return;

        logger.info("Biovotion VSM device connected.");

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

        // check for device state
        paramReadRequest(VsmConstants.PID_DEVICE_MODE);

        // check for correct algo mode
        paramReadRequest(VsmConstants.PID_ALGO_MODE);

        // check for GSR mode on
        paramReadRequest(VsmConstants.PID_GSR_ON);

        updateStatus(DeviceStatusListener.Status.CONNECTED);
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
        updateStatus(DeviceStatusListener.Status.DISCONNECTED);
    }

    @Override
    public void onVsmDeviceDisconnected(@NonNull VsmDevice device, int statusCode) {
        logger.warn("Biovotion VSM device disconnected. ({})", statusCode);
        updateStatus(DeviceStatusListener.Status.DISCONNECTED);
    }

    @Override
    public void onVsmDeviceReady(@NonNull VsmDevice device) {
        logger.info("Biovotion VSM device ready, trying to connect.");
        try {
            device.connect(VsmConstants.BLE_CONN_TIMEOUT_MS);
        } catch (NullPointerException ex) {
            logger.error("Biovotion VSM connect failed: {}", ex);
            updateStatus(DeviceStatusListener.Status.DISCONNECTED);
        }
    }


    /*
     * VsmDiscoveryListener interface
     */

    @Override
    public void onVsmDeviceFound(@NonNull Scanner scanner, @NonNull VsmDescriptor descriptor) {
        logger.info("Biovotion VSM device found.");
        vsmScanner.stopScanning();

        if (accepTopicIds.length > 0
                && !Strings.findAny(accepTopicIds, descriptor.name())
                && !Strings.findAny(accepTopicIds, descriptor.address())) {
            logger.info("Biovotion VSM Device {} with ID {} is not listed in acceptable device IDs", descriptor.name(), descriptor.address());
            getService().deviceFailedToConnect(descriptor.name());
            return;
        }

        vsmDescriptor = descriptor;
        setName(descriptor.name());

        Map<String, String> attributes = new ArrayMap<>(2);
        attributes.put("name", descriptor.name());
        attributes.put("macAddress", descriptor.address());
        attributes.put("sdk", "vsm-5.1.3-release.aar");
        // register device now, start listening to the device after the registration is successful
        getService().registerDevice(descriptor.address(), descriptor.name(), attributes);

        logger.info("Biovotion VSM device Name: {} ID: {}", descriptor.name(), descriptor.address());
    }

    @Override
    public void didRegister(AppSource source) {
        super.didRegister(source);

        getState().getId().setSourceId(source.getSourceId());

        vsmDevice = VsmDevice.sharedInstance();
        vsmDevice.setDescriptor(vsmDescriptor);

        // Bind the shared BLE service
        Intent gattServiceIntent = new Intent(getService(), BleService.class);
        bleServiceConnectionIsBound = getService().bindService(gattServiceIntent,
                bleServiceConnection, Context.BIND_AUTO_CREATE);

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
        logger.debug("Biovotion VSM Parameter written: {}", id);

        if (id == VsmConstants.PID_UTC) paramReadRequest(VsmConstants.PID_UTC);
    }

    @Override
    public void onParameterWriteError(@NonNull final ParameterController ctrl, int id, final int errorCode) {
        logger.error("Biovotion VSM Parameter write error, id={}, error={}", id, errorCode);
    }

    @Override
    public void onParameterRead(@NonNull final ParameterController ctrl, @NonNull Parameter p) {
        logger.debug("Biovotion VSM Parameter read: {}", p);

        // read device_mode parameter; if currently on charger, disconnect
        if (p.id() == VsmConstants.PID_DEVICE_MODE) {
            if (p.value()[0] == VsmConstants.MOD_ONCHARGER) {
                logger.warn("Biovotion VSM device is currently on charger, disconnecting!");
                // TODO: Maybe we can use PID_SWITCH_DEVICE_OFF here? However then it seems to not reboot automatically, so might not be able to easily reconnect afterwards...
                final Parameter disconnect = Parameter.fromBytes(VsmConstants.PID_DISCONNECT_BLE_CONN, new byte[] {(byte) 0x00});
                paramWriteRequest(disconnect);
            }
        }

        // read algo_mode parameter; if not mixed algo/raw, set it to that
        else if (p.id() == VsmConstants.PID_ALGO_MODE) {
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
                logger.debug("Biovotion VSM GAP request running");
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
        logger.debug("Biovotion VSM GAP new request: type:{} start:{} range:{}", gap_type, gap_start, gap_range);
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
        logger.debug("Biovotion VSM Data recieved: {}", unit.type);
        double timeReceived = System.currentTimeMillis() / 1000d;
        BiovotionDeviceStatus deviceStatus = getState();

        switch (unit.type) {
            case BatteryState:
                BatteryState battery = (BatteryState) unit.unit;
                logger.info("Biovotion VSM battery state: cap:{} rate:{} voltage:{} state:{}", battery.capacity, battery.chargeRate, battery.voltage/10.0f, battery.state);
                deviceStatus.setBattery(battery.capacity, battery.chargeRate, battery.voltage, battery.state);
                float[] latestBattery = deviceStatus.getBattery();

                BiovotionVsm1BatteryLevel value = new BiovotionVsm1BatteryLevel((double) unit.timestamp, timeReceived,
                        latestBattery[0], latestBattery[1], latestBattery[2], latestBattery[3]);

                trySend(batteryTopic, 0L, value);
                break;

            case Algo1:
                Algo1 algo1 = (Algo1) unit.unit;
                deviceStatus.setBloodPulseWave(algo1.bloodPulseWave, 0);
                deviceStatus.setSpo2(algo1.spO2, algo1.spO2Quality);
                deviceStatus.setHeartRate(algo1.hr, algo1.hrQuality);
                float[] latestBPW = deviceStatus.getBloodPulseWave();
                float[] latestSpo2 = deviceStatus.getSpO2();
                float[] latestHr = deviceStatus.getHeartRateAll();

                BiovotionVsm1BloodPulseWave bpwValue = new BiovotionVsm1BloodPulseWave((double) unit.timestamp, timeReceived,
                        latestBPW[0], latestBPW[1]);
                BiovotionVsm1OxygenSaturation spo2Value = new BiovotionVsm1OxygenSaturation((double) unit.timestamp, timeReceived,
                        latestSpo2[0], latestSpo2[1]);
                BiovotionVsm1HeartRate hrValue = new BiovotionVsm1HeartRate((double) unit.timestamp, timeReceived,
                        latestHr[0], latestHr[1]);

                send(bpwTopic, bpwValue);
                send(spo2Topic, spo2Value);
                send(hrTopic, hrValue);
                break;

            case Algo2:
                Algo2 algo2 = (Algo2) unit.unit;
                deviceStatus.setHeartRateVariability(algo2.hrv, algo2.hrvQuality);
                deviceStatus.setRespirationRate(algo2.respirationRate, algo2.respirationRateQuality);
                deviceStatus.setEnergy(algo2.energy, algo2.energyQuality);
                float[] latestHRV = deviceStatus.getHeartRateVariability();
                float[] latestRR = deviceStatus.getRespirationRate();
                float[] latestEnergy = deviceStatus.getEnergy();

                BiovotionVsm1HeartRateVariability hrvValue = new BiovotionVsm1HeartRateVariability((double) unit.timestamp, timeReceived,
                        latestHRV[0], latestHRV[1]);
                BiovotionVsm1RespirationRate rrValue = new BiovotionVsm1RespirationRate((double) unit.timestamp, timeReceived,
                        latestRR[0], latestRR[1]);
                BiovotionVsm1Energy energyValue = new BiovotionVsm1Energy((double) unit.timestamp, timeReceived,
                        latestEnergy[0], latestEnergy[1]);

                send(hrvTopic, hrvValue);
                send(rrTopic, rrValue);
                send(energyTopic, energyValue);
                break;

            case RawBoard:
                RawBoard rawboard = (RawBoard) unit.unit;
                deviceStatus.setTemperature(rawboard.objectTemp, rawboard.localTemp, rawboard.barometerTemp);
                deviceStatus.setGalvanicSkinResponse(rawboard.gsrAmplitude, rawboard.gsrPhase);
                float[] latestTemp = deviceStatus.getTemperatureAll();
                float[] latestGSR = deviceStatus.getGalvanicSkinResponse();

                BiovotionVsm1Temperature tempValue = new BiovotionVsm1Temperature((double) unit.timestamp, timeReceived,
                        latestTemp[0], latestTemp[1], latestTemp[2]);
                BiovotionVsm1GalvanicSkinResponse gsrValue = new BiovotionVsm1GalvanicSkinResponse((double) unit.timestamp, timeReceived,
                        latestGSR[0], latestGSR[1]);

                send(temperatureTopic, tempValue);
                send(gsrTopic, gsrValue);
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

                    BiovotionVsm1Acceleration accValue = new BiovotionVsm1Acceleration((double) unit.timestamp, timeReceived,
                            latestAcc[0], latestAcc[1], latestAcc[2]);
                    BiovotionVsm1PpgRaw ppgValue = new BiovotionVsm1PpgRaw((double) unit.timestamp, timeReceived,
                            latestPPG[0], latestPPG[1], latestPPG[2], latestPPG[3]);

                    // add measurements to a stack as long as new measurements are older. if a newer measurement is added, empty the stack into accelerationTopic and start over
                    // since acc and ppg raw data always arrive in the same unit, can just check for one and empty both
                    if (gap_raw_stack_acc.peekFirst() != null && accValue.getTime() > gap_raw_stack_acc.peekFirst().getTime()) {
                        logger.debug("Biovotion VSM Sending {} acc/ppg records", gap_raw_stack_acc.size());
                        while (!gap_raw_stack_acc.isEmpty()) {
                            send(accelerationTopic, gap_raw_stack_acc.removeFirst());
                            send(ppgRawTopic, gap_raw_stack_ppg.removeFirst());
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

                BiovotionVsm1LedCurrent ledValue = new BiovotionVsm1LedCurrent((double) unit.timestamp, timeReceived,
                    latestLedCurrent[0], latestLedCurrent[1], latestLedCurrent[2], latestLedCurrent[3]);

                // add measurements to a stack as long as new measurements are older. if a newer measurement is added, empty the stack into ledCurrentTopic and start over
                if (gap_raw_stack_led_current.peekFirst() != null && ledValue.getTime() > gap_raw_stack_led_current.peekFirst().getTime()) {
                    logger.debug("Biovotion VSM Sending {} led current records", gap_raw_stack_acc.size());
                    while (!gap_raw_stack_led_current.isEmpty()) {
                        send(ledCurrentTopic, gap_raw_stack_led_current.removeFirst());
                    }
                }
                gap_raw_stack_led_current.addFirst(ledValue);

                break;
        }
    }

    @Override
    protected void registerDeviceAtReady() {
        // custom registration
    }
}
