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
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.biovotion.BiovotionVsm1HeartRate;
import org.radarcns.topic.AvroTopic;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

/**
 * Shows recently collected heartbeats in a Toast.
 */
public class BiovotionHeartbeatToast extends AsyncTask<DeviceServiceConnection<BiovotionDeviceStatus>, Void, String[]> {
    private final Context context;
    private static final DecimalFormat singleDecimal = new DecimalFormat("0.0");
    private static final AvroTopic<ObservationKey, BiovotionVsm1HeartRate> topic = BiovotionTopics
            .getInstance().getHeartRateTopic();

    public BiovotionHeartbeatToast(Context context) {
        this.context = context;
    }

    @Override
    @SafeVarargs
    protected final String[] doInBackground(DeviceServiceConnection<BiovotionDeviceStatus>... params) {
        String[] results = new String[params.length];
        for (int i = 0; i < params.length; i++) {
            try {
                List<Record<ObservationKey, BiovotionVsm1HeartRate>> measurements = params[i].getRecords(topic, 25);
                if (!measurements.isEmpty()) {
                    StringBuilder sb = new StringBuilder(3200); // <32 chars * 100 measurements
                    for (Record<ObservationKey, BiovotionVsm1HeartRate> measurement : measurements) {
                        long diffTimeMillis = System.currentTimeMillis() - (long) (1000d * measurement.value.getTimeReceived());
                        sb.append(singleDecimal.format(diffTimeMillis / 1000d));
                        sb.append(" sec. ago: ");
                        sb.append(singleDecimal.format(measurement.value.getHeartRate()));
                        sb.append(" bpm\n");
                    }
                    results[i] = sb.toString();
                } else {
                    results[i] = null;
                }
            } catch (IOException e) {
                results[i] = null;
            }
        }
        return results;
    }

    @Override
    protected void onPostExecute(String[] strings) {
        for (String s : strings) {
            if (s == null) {
                Boast.makeText(context, "No heart rate collected yet.", Toast.LENGTH_SHORT).show();
            } else {
                Boast.makeText(context, s, Toast.LENGTH_LONG).show();
            }
        }
    }
}
