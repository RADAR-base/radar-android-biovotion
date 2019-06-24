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

import android.content.Context;
import android.os.AsyncTask;
import android.widget.Toast;
import org.radarcns.android.device.DeviceServiceConnection;
import org.radarcns.android.util.Boast;
import org.radarcns.data.Record;
import org.radarcns.data.RecordData;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.biovotion.BiovotionVsm1HeartRate;
import org.radarcns.topic.AvroTopic;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

/**
 * Shows recently collected heartbeats in a Toast.
 */
public class BiovotionHeartbeatToast extends AsyncTask<Void, Void, String> {
    private static final DecimalFormat singleDecimal = new DecimalFormat("0.0");
    private final DeviceServiceConnection<BiovotionDeviceStatus> connection;

    public BiovotionHeartbeatToast(DeviceServiceConnection<BiovotionDeviceStatus> connection) {
        this.connection = connection;
    }

    @Override
    @SafeVarargs
    protected final String doInBackground(Void... params) {
        try {
                RecordData<Object, Object> measurements = connection.getRecords("android_biovotion_vsm1_heartrate", 25);
                if (!measurements.isEmpty()) {
                    StringBuilder sb = new StringBuilder(3200); // <32 chars * 100 measurements
                    for (Object measurement : measurements) {
                        BiovotionVsm1HeartRate HR_measurement = (BiovotionVsm1HeartRate) measurement;
                        long diffTimeMillis = System.currentTimeMillis() - (long) (1000d * HR_measurement.getTimeReceived());
                        sb.append(singleDecimal.format(diffTimeMillis / 1000d));
                        sb.append(" sec. ago: ");
                        sb.append(singleDecimal.format(HR_measurement.getHeartRate()));
                        sb.append(" bpm\n");
                    }
                    return sb.toString();
                }
        } catch (IOException ignore) {
        }

        return "No heart rate collected yet.";
    }

    @Override
    protected void onPostExecute(String result) {
        Boast.makeText(connection.getContext(), result, Toast.LENGTH_LONG).show();
    }
}
