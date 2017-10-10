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

import org.radarcns.android.device.DeviceTopics;
import org.radarcns.topic.AvroTopic;
import org.radarcns.key.MeasurementKey;

/** Topic manager for topics concerning the Biovotion VSM. */
public class BiovotionTopics extends DeviceTopics {
    private final AvroTopic<MeasurementKey, BiovotionVSMBatteryState> batteryStateTopic;
    private final AvroTopic<MeasurementKey, BiovotionVSMBloodPulseWave> bloodPulseWaveTopic;
    private final AvroTopic<MeasurementKey, BiovotionVSMSpO2> spo2Topic;
    private final AvroTopic<MeasurementKey, BiovotionVSMHeartRate> heartRateTopic;
    private final AvroTopic<MeasurementKey, BiovotionVSMHeartRateVariability> hrvTopic;
    private final AvroTopic<MeasurementKey, BiovotionVSMRespirationRate> rrTopic;
    private final AvroTopic<MeasurementKey, BiovotionVSMEnergy> energyTopic;
    private final AvroTopic<MeasurementKey, BiovotionVSMTemperature> temperatureTopic;
    private final AvroTopic<MeasurementKey, BiovotionVSMGalvanicSkinResponse> gsrTopic;
    private final AvroTopic<MeasurementKey, BiovotionVSMAcceleration> accelerationTopic;
    private final AvroTopic<MeasurementKey, BiovotionVSMPhotoRaw> ppgRawTopic;
    private final AvroTopic<MeasurementKey, BiovotionVSMLedCurrent> ledCurrentTopic;

    private static final Object syncObject = new Object();
    private static BiovotionTopics instance = null;

    public static BiovotionTopics getInstance() {
        synchronized (syncObject) {
            if (instance == null) {
                instance = new BiovotionTopics();
            }
            return instance;
        }
    }

    private BiovotionTopics() {
        batteryStateTopic = createTopic("android_biovotion_battery_state",
                BiovotionVSMBatteryState.getClassSchema(),
                BiovotionVSMBatteryState.class);
        bloodPulseWaveTopic = createTopic("android_biovotion_blood_pulse_wave",
                BiovotionVSMBloodPulseWave.getClassSchema(),
                BiovotionVSMBloodPulseWave.class);
        spo2Topic = createTopic("android_biovotion_spo2",
                BiovotionVSMSpO2.getClassSchema(),
                BiovotionVSMSpO2.class);
        heartRateTopic = createTopic("android_biovotion_heart_rate",
                BiovotionVSMHeartRate.getClassSchema(),
                BiovotionVSMHeartRate.class);
        hrvTopic = createTopic("android_biovotion_heart_rate_variability",
                BiovotionVSMHeartRateVariability.getClassSchema(),
                BiovotionVSMHeartRateVariability.class);
        rrTopic = createTopic("android_biovotion_respiration_rate",
                BiovotionVSMRespirationRate.getClassSchema(),
                BiovotionVSMRespirationRate.class);
        energyTopic = createTopic("android_biovotion_energy",
                BiovotionVSMEnergy.getClassSchema(),
                BiovotionVSMEnergy.class);
        temperatureTopic = createTopic("android_biovotion_temperature",
                BiovotionVSMTemperature.getClassSchema(),
                BiovotionVSMTemperature.class);
        gsrTopic = createTopic("android_biovotion_galvanic_skin_response",
                BiovotionVSMGalvanicSkinResponse.getClassSchema(),
                BiovotionVSMGalvanicSkinResponse.class);
        accelerationTopic = createTopic("android_biovotion_acceleration",
                BiovotionVSMAcceleration.getClassSchema(),
                BiovotionVSMAcceleration.class);
        ppgRawTopic = createTopic("android_biovotion_ppg_raw",
                BiovotionVSMPhotoRaw.getClassSchema(),
                BiovotionVSMPhotoRaw.class);
        ledCurrentTopic = createTopic("android_biovotion_led_current",
                BiovotionVSMLedCurrent.getClassSchema(),
                BiovotionVSMLedCurrent.class);
    }

    public AvroTopic<MeasurementKey, BiovotionVSMBatteryState> getBatteryStateTopic() {
        return batteryStateTopic;
    }

    public AvroTopic<MeasurementKey, BiovotionVSMBloodPulseWave> getBloodPulseWaveTopic() {
        return bloodPulseWaveTopic;
    }

    public AvroTopic<MeasurementKey, BiovotionVSMSpO2> getSpO2Topic() {
        return spo2Topic;
    }

    public AvroTopic<MeasurementKey, BiovotionVSMHeartRate> getHeartRateTopic() {
        return heartRateTopic;
    }

    public AvroTopic<MeasurementKey, BiovotionVSMHeartRateVariability> getHrvTopic() {
        return hrvTopic;
    }

    public AvroTopic<MeasurementKey, BiovotionVSMRespirationRate> getRrTopic() {
        return rrTopic;
    }

    public AvroTopic<MeasurementKey, BiovotionVSMEnergy> getEnergyTopic() {
        return energyTopic;
    }

    public AvroTopic<MeasurementKey, BiovotionVSMTemperature> getTemperatureTopic() {
        return temperatureTopic;
    }

    public AvroTopic<MeasurementKey, BiovotionVSMGalvanicSkinResponse> getGsrTopic() {
        return gsrTopic;
    }

    public AvroTopic<MeasurementKey, BiovotionVSMAcceleration> getAccelerationTopic() {
        return accelerationTopic;
    }

    public AvroTopic<MeasurementKey, BiovotionVSMPhotoRaw> getPhotoRawTopic() {
        return ppgRawTopic;
    }

    public AvroTopic<MeasurementKey, BiovotionVSMLedCurrent> getLedCurrentTopic() {
        return ledCurrentTopic;
    }
}
