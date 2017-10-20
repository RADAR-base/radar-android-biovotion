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

/**
 * Define some constants used in several activities and views.
 *
 * @author Christopher Métrailler (mei@hevs.ch)
 * @version 1.0 - 2016/07/20
 */
public final class VsmConstants {

    /**
     * Key used to pass the DiscoveredEntity descriptor as intent argument.
     * The descriptor of the discovered VSM device is serialized and passed between activities.
     */
    public final static String KEY_DESC_EXTRA = "key_extra_desc";

    /**
     * sharedPreferences keys for storage of GAP request info
     */
    public static final String BIOVOTION_PREFS = "org.radarcns.biovotion";
    public static final String GAP_LAST_INDEX = "org.radarcns.biovotion.gapLastIndex";

    /**
     * Default Bluetooth connection timeout. If this timeout is reached, the callback
     * VsmDeviceListener#onVsmDeviceConnectionError(VsmDevice, VsmConnectionState) is called.
     *
     * <p>
     * The default timeout on Android is 30 seconds. Using the BLE library, the maximum timeout value
     * is fixed to 25 seconds.
     * When connecting the first time to a VSM device, the connection time can be up to 5 seconds because the pairing
     * process can take time and it is based on some internal delays.
     */
    public final static int BLE_CONN_TIMEOUT_MS = 10000;


    /**
     * Parameter IDs according to VSM Bluetooth Comms Spec
     */
    public final static int PID_INTERFACE_VERSION                     = 0x00; // R
    public final static int PID_UTC                                   = 0x01; // RW
    public final static int PID_ALGO_MODE                             = 0x04; // RW
    public final static int PID_DEVICE_MODE                           = 0x05; // R
    public final static int PID_FIRMWARE_M4_VERSION                   = 0x06; // R
    public final static int PID_FIRMWARE_AS7000_VERSION               = 0x08; // R
    public final static int PID_HW_VERSION                            = 0x09; // R
    public final static int PID_FIRMWARE_BLE_VERSION                  = 0x0B; // R
    public final static int PID_UNIQUE_DEVICE_ID                      = 0x12; // R
    public final static int PID_STORAGE_TIME                          = 0x15; // RW
    public final static int PID_NO_AVERAGE_PER_SAMPLE_AS7000          = 0x16; // RW
    public final static int PID_GSR_ON                                = 0x1D; // RW
    public final static int PID_ADVERTISING_NAME                      = 0x1E; // RW
    public final static int PID_DTM_MODE                              = 0x1F; // W
    public final static int PID_DISABLE_LOW_BAT_NOTIFICATION          = 0x20; // RW
    public final static int PID_ENERGY_SUM_HOURS                      = 0x22; // R
    public final static int PID_STEPS_SUM_HOURS                       = 0x23; // R
    public final static int PID_SWITCH_DEVICE_OFF                     = 0x2A; // W
    public final static int PID_TX_POWER                              = 0x2B; // RW
    public final static int PID_SET_FACTORY_DEFAULT                   = 0x2C; // W
    public final static int PID_DISCONNECT_BLE_CONN                  = 0x2D; // W
    public final static int PID_SELF_TEST_RESULT                      = 0x2E; // R
    public final static int PID_CLEAR_WHITE_LIST                      = 0x30; // W
    public final static int PID_BOOTLOADER_VERSIOM                    = 0x31; // R
    public final static int PID_BL_KEY_META_DATA                      = 0x32; // R
    public final static int PID_DISABLE_DOUBLE_TAP_NOTIFICATION       = 0x33; // RW

    // GAP Request
    public final static int PID_GAP_REQUEST_STATUS                    = 0x21; // R
    public final static int PID_GAP_RECORD_REQUEST                    = 0x11; // W
    public final static int PID_SET_LAST_COUNTER_VALUE                = 0x2F; // W

    public final static int PID_LAST_ERROR_COUNTER_VALUE              = 0x0F; // R
    public final static int PID_LAST_LOG_COUNTER_VALUE                = 0x10; // R
    public final static int PID_LAST_VITAL_DATA_COUNTER_VALUE         = 0x0E; // R
    public final static int PID_LAST_IPI_DATA_COUNTER_VALUE           = 0x27; // R
    public final static int PID_LAST_RAW_COUNTER_VALUE                = 0x24; // R

    public final static int PID_NUMBER_OF_ERROR_LOG_SETS_IN_STORAGE   = 0x1B; // R
    public final static int PID_NUMBER_OF_LOG_SETS_IN_STORAGE         = 0x19; // R
    public final static int PID_NUMBER_OF_VITAL_DATA_SETS_IN_STORAGE  = 0x17; // R
    public final static int PID_NUMBER_OF_IPI_DATA_SETS_IN_STORAGE    = 0x28; // R
    public final static int PID_NUMBER_OF_RAW_DATA_SETS_IN_STORAGE    = 0x25; // R

    /**
     * GAP request data types
     */
    public final static int GAP_TYPE_ERROR_LOG    = 0x00;
    public final static int GAP_TYPE_EVENT_LOG    = 0x01;
    public final static int GAP_TYPE_VITAL        = 0x02;
    public final static int GAP_TYPE_IPI          = 0x03;
    public final static int GAP_TYPE_VITAL_RAW    = 0x10;

    /**
     * GAP request maximum number of records per page, whole pages will be streamed if possible
     */
    public final static int GAP_MAX_PER_PAGE_ERROR_LOG    = 0x0C;
    public final static int GAP_MAX_PER_PAGE_EVENT_LOG    = 0x09;
    public final static int GAP_MAX_PER_PAGE_VITAL        = 0x09;
    public final static int GAP_MAX_PER_PAGE_IPI          = 0x27;
    public final static int GAP_MAX_PER_PAGE_VITAL_RAW    = 0x11;

    /**
     * GAP request responses
     */
    public final static int GAP_RESPONSE_PARAMETER_STATUS_OK        = 0x00;
    public final static int GAP_RESPONSE_PARAMETER_OUT_OF_RANGE     = 0x03;
    public final static int GAP_RESPONSE_BUSY                       = 0x05;
    public final static int GAP_RESPONSE_UNKNOWN_STORAGE            = 0x06;
    public final static int GAP_RESPONSE_NO_DATA                    = 0x07;
    public final static int GAP_RESPONSE_INTERNAL_ERROR             = 0x08;

    /**
     * GAP statuses
     */
    public final static int GAP_STATUS_RUNNING              = 0x00;
    public final static int GAP_STATUS_SUCCESSFUL           = 0x01;
    public final static int GAP_STATUS_ERROR_NO_MORE_DATA   = 0x02;
    public final static int GAP_STATUS_ERROR_NO_PAGE        = 0x03;
    public final static int GAP_STATUS_ERROR_OVERFLOW       = 0x04;
    public final static int GAP_STATUS_ERROR_BLE            = 0x05;
    public final static int GAP_STATUS_ERROR_NOMAIL         = 0x06;
    public final static int GAP_STATUS_FOTA_RUNNING         = 0x07;

    /**
     * GAP request miscellaneous
     */
    public final static int GAP_MAX_PAGES     = 50; // max number of pages to get with one request
    public final static int GAP_INTERVAL_MS   = 500; // try a new GAP request every x milliseconds


    /**
     * UTC set time interval
     */
    public final static int UTC_INTERVAL_MS   = 60000; // set the device UTC time every x milliseconds


    /**
     * VSM algorithm modes
     */
    public final static int MOD_VITAL_MODE                = 0x00;
    public final static int MOD_VITAL_CAPPED_MODE         = 0x01;
    public final static int MOD_RAW_DATA_VITAL_MODE       = 0x04;
    public final static int MOD_RAW_DATA_HR_ONLY_MODE     = 0x05;
    public final static int MOD_SELF_TEST_MODE            = 0x06;
    public final static int MOD_MIXED_VITAL_RAW           = 0x09;
    public final static int MOD_VITAL_MODE_AUTO_DATA      = 0x0A;
    public final static int MOD_GREEN_ONLY_MODE           = 0x0B;
    public final static int MOD_RAW_DATA_FIX_CURRENT      = 0x0C;
    public final static int MOD_SHORT_SELF_TEST_MODE      = 0x0D;
    public final static int MOD_MIXED_VITAL_RAW_SILENT    = 0x0E;


    /**
     * VSM device modes
     */
    public final static int MOD_IDLE            = 0x00;
    public final static int MOD_MEASURE         = 0x01;
    public final static int MOD_ONCHARGER       = 0x02;


    private VsmConstants() {
        // Private
    }
}
