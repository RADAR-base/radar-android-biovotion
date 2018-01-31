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
import android.bluetooth.BluetoothDevice;
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
import ch.hevs.ble.lib.events.commands.BleCommand;
import ch.hevs.ble.lib.events.commands.CommandStatus;
import ch.hevs.ble.lib.events.responses.BleGattResponse;
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
    private BiovotionGAPManager gapManager;

    private Deque<BiovotionVsm1Acceleration> gap_raw_stack_acc;
    private Deque<BiovotionVsm1PpgRaw> gap_raw_stack_ppg;
    private Deque<BiovotionVsm1LedCurrent> gap_raw_stack_led_current;

    private ScheduledFuture<?> utcFuture;
    private final ByteBuffer utcBuffer;

    // Code to manage Service lifecycle.
    private final BleServiceObserver vsmBleServiceObserver = new BleServiceObserver() {
        @Override
        public void onGattResponse(@NonNull BluetoothDevice bluetoothDevice, @NonNull BleGattResponse bleGattResponse) {
            //logger.debug("Biovotion VSM BLE service ({}) GATT response {}", bluetoothDevice, bleGattResponse);
        }

        @Override
        public void onCommandError(BluetoothDevice bluetoothDevice, @NonNull BleCommand bleCommand, @NonNull CommandStatus commandStatus) {
            logger.error("Biovotion VSM BLE service ({}) command error {}", bluetoothDevice, commandStatus);
        }

        @Override
        public void onGattError(BluetoothDevice bluetoothDevice, @NonNull BleGattResponse bleGattResponse, int i) {
            logger.error("Biovotion VSM BLE service ({}) GATT error {} {}", bluetoothDevice, i, bleGattResponse);
        }

        @Override
        public void onConnectionError(BluetoothDevice bluetoothDevice, @NonNull ConnectionErrorState connectionErrorState) {
            logger.error("Biovotion VSM BLE service ({}) connection error {}", bluetoothDevice, connectionErrorState);
        }

        @Override
        public void onConnectionStateChanged(BluetoothDevice bluetoothDevice, @NonNull ConnectionState connectionState) {
            logger.debug("Biovotion VSM BLE service ({}) connection state changed ({})", bluetoothDevice, connectionState);
        }

        @Override
        public void onBondingStateChanged(@NonNull BluetoothDevice bluetoothDevice, @NonNull BondingState bondingState) {
            logger.debug("Biovotion VSM BLE service ({}) bonding state changed ({})", bluetoothDevice, bondingState);
        }
    };
    private boolean bleServiceConnectionIsBound;
    private final ServiceConnection bleServiceConnection = new ServiceConnection() {

        @Override
        public void onServiceConnected(ComponentName name, IBinder service) {
            vsmBleService = ((BleService.LocalBinder) service).getService();
            vsmBleService.setVerbose(true);

            // The shared BLE service is now connected.
            vsmDevice.setBleService(vsmBleService);
            vsmBleService.registerObserver(vsmBleServiceObserver);

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

        this.gapManager = new BiovotionGAPManager(getService());

        this.gap_raw_stack_acc = new ArrayDeque<>(1024);
        this.gap_raw_stack_ppg = new ArrayDeque<>(1024);
        this.gap_raw_stack_led_current = new ArrayDeque<>(1024);

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

    // Called when a device is disconnected. Does some housekeeping actions.
    // Should not be called from multiple threads at the same time!
    public synchronized void disconnect() {
        logger.info("Biovotion VSM Manager disconnecting.");
        this.isConnected = false;

        // stop device/ble services
        if (vsmScanner != null && vsmScanner.isScanning()) {
            vsmScanner.stopScanning();
            vsmScanner = null;
            logger.debug("Biovotion VSM disconnect: vsmScanner.");
        }
        if (vsmDevice != null) {
            if (vsmDevice.isConnected()) vsmDevice.disconnect();
            vsmDevice.removeListeners();
            vsmDevice = null;
            logger.debug("Biovotion VSM disconnect: vsmDevice.");
        }
        if (vsmBleService != null){// && vsmBleService.connectionState() == BleServiceObserver.ConnectionState.GATT_CONNECTED) {
            vsmBleService.disconnect();
            if (bleServiceConnectionIsBound) {
                getService().unbindService(bleServiceConnection);
                bleServiceConnectionIsBound = false;
            }
            vsmBleService = null;
            logger.debug("Biovotion VSM disconnect: vsmBleService.");
        }

        // remove controllers
        if (vsmStreamController != null) {
            vsmStreamController.removeListeners();
            vsmStreamController = null;
            logger.debug("Biovotion VSM disconnect: vsmStreamController.");
        }
        if (vsmParameterController != null) {
            vsmParameterController.removeListeners();
            vsmParameterController = null;
            logger.debug("Biovotion VSM disconnect: vsmParameterController.");
        }

        // empty remaining raw data stacks
        while (!gap_raw_stack_acc.isEmpty()) {
            send(accelerationTopic, gap_raw_stack_acc.removeFirst());
            send(ppgRawTopic, gap_raw_stack_ppg.removeFirst());
            logger.debug("Biovotion VSM disconnect: empty acc raw stack.");
        }
        while (!gap_raw_stack_led_current.isEmpty()) {
            send(ledCurrentTopic, gap_raw_stack_led_current.removeFirst());
            logger.debug("Biovotion VSM disconnect: empty led current raw stack.");
        }

        logger.info("Biovotion VSM Manager disconnected.");
    }

    // Check if the device is on the blacklist. Remove if timeout condition is met.
    public boolean isBlacklisted(String address) {
        Map<String, Long> deviceBlacklist = getService().getDeviceBlacklist();
        if (deviceBlacklist.containsKey(address)) {
            if (System.currentTimeMillis() - deviceBlacklist.get(address) > VsmConstants.BLE_BLACKLIST_TIMEOUT_MS) {
                getService().getDeviceBlacklist().remove(address);
                return false;
            } else {
                logger.debug("Biovotion VSM Device with ID {} is on the blacklist for a remaining {}s.", address
                        , (VsmConstants.BLE_BLACKLIST_TIMEOUT_MS - System.currentTimeMillis() + deviceBlacklist.get(address)) / 1000d);
                logger.trace("Biovotion VSM device blacklist: {}", deviceBlacklist);
                return true;
            }
        }
        return false;
    }

    public void updateBlacklist(String address) {
        getService().getDeviceBlacklist().put(address, System.currentTimeMillis());
        logger.debug("Biovotion VSM Device with ID {} was put on the blacklist.", address);
        logger.trace("Biovotion VSM device blacklist: {}", getService().getDeviceBlacklist());
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
        if (status == getState().getStatus()) return;
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

        logger.info("Biovotion VSM device {} connected.", device.descriptor().name());

        vsmScanner.stopScanning();
        vsmDevice = device;

        vsmStreamController = vsmDevice.streamController();
        vsmStreamController.addListener(this);
        vsmParameterController = vsmDevice.parameterController();
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

        gapManager.setDeviceId(vsmDevice.descriptor().address());

        updateStatus(DeviceStatusListener.Status.CONNECTED);
        this.isConnected = true;
    }

    @Override
    public void onVsmDeviceConnecting(@NonNull VsmDevice device) {
        logger.info("Biovotion VSM device {} connecting.", device.descriptor().name());
        setName(device.descriptor().name());
        updateStatus(DeviceStatusListener.Status.CONNECTING);
    }

    @Override
    public void onVsmDeviceConnectionError(@NonNull VsmDevice device, VsmConnectionState errorState) {
        logger.error("Biovotion VSM device connection error for {}: {}", device.descriptor().name(), errorState.toString());
        getService().deviceFailedToConnect(device.descriptor().name());
        //vsmScanner.startScanning();
        updateStatus(DeviceStatusListener.Status.DISCONNECTED);
    }

    @Override
    public void onVsmDeviceDisconnected(@NonNull VsmDevice device, int statusCode) {
        logger.warn("Biovotion VSM device {} disconnected. ({})", device.descriptor().name(), statusCode);
        if (statusCode != 0) {
            updateBlacklist(device.descriptor().address());
        }
        updateStatus(DeviceStatusListener.Status.DISCONNECTED);
    }

    @Override
    public void onVsmDeviceReady(@NonNull VsmDevice device) {
        logger.info("Biovotion VSM device ready, trying to connect {}.", device.descriptor().name());

        if (isBlacklisted(device.descriptor().address())) return;

        try {
            device.connect(VsmConstants.BLE_CONN_TIMEOUT_MS);
        } catch (NullPointerException ex) {
            logger.error("Biovotion VSM connect failed: {}", ex);
            getService().deviceFailedToConnect(device.descriptor().name());
            updateStatus(DeviceStatusListener.Status.DISCONNECTED);
        }
    }


    /*
     * VsmDiscoveryListener interface
     */

    @Override
    public void onVsmDeviceFound(@NonNull Scanner scanner, @NonNull VsmDescriptor descriptor) {
        logger.debug("Biovotion VSM device {} found with ID: {}", descriptor.name(), descriptor.address());

        if (isBlacklisted(descriptor.address())) return;

        if (accepTopicIds.length > 0
                && !Strings.findAny(accepTopicIds, descriptor.name())
                && !Strings.findAny(accepTopicIds, descriptor.address())) {
            logger.info("Biovotion VSM Device {} with ID {} is not listed in acceptable device IDs", descriptor.name(), descriptor.address());
            updateBlacklist(descriptor.address());
            return;
        }

        vsmDescriptor = descriptor;
        setName(vsmDescriptor.name());

        Map<String, String> attributes = new ArrayMap<>(2);
        attributes.put("name", vsmDescriptor.name());
        attributes.put("macAddress", vsmDescriptor.address());
        attributes.put("sdk", "vsm-5.2.0-release.aar");
        // register device now, start listening to the device after the registration is successful
        getService().registerDevice(vsmDescriptor.address(), vsmDescriptor.name(), attributes);
    }

    @Override
    public void didRegister(AppSource source) {
        super.didRegister(source);

        getState().getId().setSourceId(source.getSourceId());

        vsmDevice = VsmDevice.sharedInstance();
        vsmDevice.setDescriptor(vsmDescriptor);

        // Bind the shared BLE service. This actually starts the connection process.
        Intent gattServiceIntent = new Intent(getService(), BleService.class);
        bleServiceConnectionIsBound = getService().bindService(gattServiceIntent,
                bleServiceConnection, Context.BIND_AUTO_CREATE);

        if (this != null) {
            vsmDevice.addListener(this);
        }
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
        else if (id == VsmConstants.PID_GAP_RECORD_REQUEST) gapManager.rawGapSuccessful();
        // else if (id == VsmConstants.PID_GAP_RECORD_REQUEST) gapManager.getRawGap().setGapResponse(?); TODO: set GAP response, currently not implemeted by Biovotion?
    }

    @Override
    public void onParameterWriteError(@NonNull final ParameterController ctrl, int id, final int errorCode) {
        logger.error("Biovotion VSM Parameter write error, id={}, error={}", id, errorCode);

        if (id == VsmConstants.PID_GAP_RECORD_REQUEST) gapManager.setStatus(1); // reset manual override status after GAP error
    }

    @Override
    public void onParameterRead(@NonNull final ParameterController ctrl, @NonNull Parameter p) {
        logger.debug("Biovotion VSM Parameter read: {}", p);

        // read device_mode parameter; if currently on charger, disconnect
        if (p.id() == VsmConstants.PID_DEVICE_MODE) {
            if (p.value()[0] == VsmConstants.MOD_ONCHARGER
                    && gapManager.getRawGap().getGapStreamLag() >= 0
                    && gapManager.getRawGap().getGapStreamLag() < VsmConstants.GAP_MAX_PAGES*VsmConstants.GAP_MAX_PER_PAGE_VITAL_RAW) {
                logger.warn("Biovotion VSM device is currently on charger and not uploading local data, disconnecting!");
                logger.warn("Biovotion VSM GAP status: {}", gapManager);

                updateBlacklist(vsmDevice.descriptor().address());

                vsmDevice.disconnect(); // properly disconnects, internally also writes to parameter PID_DISCONNECT_BLE_CONN
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


        /*
         * RAW GAP REQUEST
         */

        // read GAP request status
        else if (p.id() == VsmConstants.PID_GAP_REQUEST_STATUS) {
            gapManager.setStatus(p.value()[0]);

            logger.debug("Biovotion VSM GAP status: {}", gapManager);

            if (gapManager.isFirstRawRequest()) {
                logger.info("Biovotion VSM GAP first request, ignoring status");
            } else if (gapManager.getStatus() == 0) {
                logger.debug("Biovotion VSM GAP request running");
                // HACK: check if last GAP request was longer than GAP_MAX_REQUEST_MS ago, disconnect
                logger.trace("Biovotion VSM GAP request running since: {}, now: {}, runtime: {}"
                        , gapManager.getRawGap().getLastGapTime(), System.currentTimeMillis(), (System.currentTimeMillis() - gapManager.getRawGap().getLastGapTime())/1000L);
                if (System.currentTimeMillis() - gapManager.getRawGap().getLastGapTime() > VsmConstants.GAP_MAX_REQUEST_MS) {
                    logger.error("Biovotion VSM GAP request running too long, disconnecting!");
                    vsmDevice.disconnect();
                }
                // abort if GAP already running
                return;
            } else if (gapManager.getStatus() > 1) {
                logger.warn("Biovotion VSM GAP request status abnormal: {}", gapManager.getStatus());
            }

            paramReadRequest(VsmConstants.PID_NUMBER_OF_RAW_DATA_SETS_IN_STORAGE);
        }

        // read number of raw data sets currently in device storage
        else if (p.id() == VsmConstants.PID_NUMBER_OF_RAW_DATA_SETS_IN_STORAGE) {
            gapManager.getRawGap().setGapNum(p.valueAsInteger());

            logger.debug("Biovotion VSM GAP status: {}", gapManager);

            paramReadRequest(VsmConstants.PID_LAST_RAW_COUNTER_VALUE);
        }

        // read current latest record counter id. may initiate GAP request here
        else if (p.id() == VsmConstants.PID_LAST_RAW_COUNTER_VALUE) {
            gapManager.getRawGap().setGapCount(p.valueAsInteger());

            logger.info("Biovotion VSM GAP status: {}", gapManager);

            // GAP request here
            Parameter rawGapReq = gapManager.newRawGap();
            gapManager.getRawGap().setLastGapRequest(rawGapReq, System.currentTimeMillis());
            if (rawGapReq != null && !paramWriteRequest(rawGapReq)) {
                logger.error("Biovotion VSM GAP raw request failed!");
            } else {
                logger.debug("Biovotion VSM GAP raw request created was null, skipping.");
            }
        }
    }

    @Override
    public void onParameterReadError(@NonNull final ParameterController ctrl, int id) {
        logger.error("Biovotion VSM Parameter read error, id={}", id);
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
    public void onStreamMessageReceived(@NonNull final java.nio.ByteBuffer payload) {
        logger.trace("Biovotion VSM Message received: {}", payload);
        //TODO: find out what is sent here
    }

    @Override
    public void onStreamValueReceived(@NonNull final StreamValue unit) {
        logger.trace("Biovotion VSM Data received: {}", unit.type);
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

                    if (!VsmConstants.VSM_REVERSE_RAW_DATA) {
                        send(accelerationTopic, accValue);
                        send(ppgRawTopic, ppgValue);
                    } else {
                        // add measurements to a stack as long as new measurements are older. if a newer measurement is added, empty the stack into accelerationTable and start over
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
                    }

                    gapManager.getRawGap().incGapSinceLast();
                }
                break;

            case LedCurrent:
                LedCurrent ledcurrent = (LedCurrent) unit.unit;
                //logger.info("Biovotion VSM LedCurrent: red:{} | green:{} | ir:{} | offset:{}", ledcurrent.red, ledcurrent.green, ledcurrent.ir, ledcurrent.offset);
                deviceStatus.setLedCurrent(ledcurrent.red, ledcurrent.green, ledcurrent.ir, ledcurrent.offset);
                float[] latestLedCurrent = deviceStatus.getLedCurrent();

                BiovotionVsm1LedCurrent ledValue = new BiovotionVsm1LedCurrent((double) unit.timestamp, timeReceived,
                    latestLedCurrent[0], latestLedCurrent[1], latestLedCurrent[2], latestLedCurrent[3]);

                if (!VsmConstants.VSM_REVERSE_RAW_DATA) {
                    send(ledCurrentTopic, ledValue);
                } else {
                    // add measurements to a stack as long as new measurements are older. if a newer measurement is added, empty the stack into ledCurrentTable and start over
                    if (gap_raw_stack_led_current.peekFirst() != null && ledValue.getTime() > gap_raw_stack_led_current.peekFirst().getTime()) {
                        logger.debug("Biovotion VSM Sending {} led current records", gap_raw_stack_acc.size());
                        while (!gap_raw_stack_led_current.isEmpty()) {
                            send(ledCurrentTopic, gap_raw_stack_led_current.removeFirst());
                        }
                    }
                    gap_raw_stack_led_current.addFirst(ledValue);
                }

                break;
        }
    }

    @Override
    protected void registerDeviceAtReady() {
        // custom registration
    }
}
