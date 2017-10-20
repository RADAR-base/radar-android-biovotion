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
import org.radarcns.kafka.ObservationKey;

/** Topic manager for topics concerning the Biovotion VSM. */
public class BiovotionTopics extends DeviceTopics {
    private final AvroTopic<ObservationKey, BiovotionVsm1BatteryLevel> batteryStateTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1BloodPulseWave> bloodPulseWaveTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1OxygenSaturation> spo2Topic;
    private final AvroTopic<ObservationKey, BiovotionVsm1HeartRate> heartRateTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1HeartRateVariability> hrvTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1RespirationRate> respirationTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1Energy> energyTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1Temperature> temperatureTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1GalvanicSkinResponse> gsrTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1Acceleration> accelerationTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1PpgRaw> ppgRawTopic;
    private final AvroTopic<ObservationKey, BiovotionVsm1LedCurrent> ledCurrentTopic;

    private static BiovotionTopics instance = null;

    public synchronized static BiovotionTopics getInstance() {
        if (instance == null) {
            instance = new BiovotionTopics();
        }
        return instance;
    }

    private BiovotionTopics() {
        batteryStateTopic = createTopic("android_biovotion_vsm1_battery_level",
                BiovotionVsm1BatteryLevel.class);
        bloodPulseWaveTopic = createTopic("android_biovotion_vsm1_blood_volume_pulse",
                BiovotionVsm1BloodPulseWave.class);
        spo2Topic = createTopic("android_biovotion_vsm1_oxygen_saturation",
                BiovotionVsm1OxygenSaturation.class);
        heartRateTopic = createTopic("android_biovotion_vsm1_heartrate",
                BiovotionVsm1HeartRate.class);
        hrvTopic = createTopic("android_biovotion_vsm1_heartrate_variability",
                BiovotionVsm1HeartRateVariability.class);
        respirationTopic = createTopic("android_biovotion_vsm1_respiration_rate",
                BiovotionVsm1RespirationRate.class);
        energyTopic = createTopic("android_biovotion_vsm1_energy",
                BiovotionVsm1Energy.class);
        temperatureTopic = createTopic("android_biovotion_vsm1_temperature",
                BiovotionVsm1Temperature.class);
        gsrTopic = createTopic("android_biovotion_vsm1_galvanic_skin_response",
                BiovotionVsm1GalvanicSkinResponse.class);
        accelerationTopic = createTopic("android_biovotion_vsm1_acceleration",
                BiovotionVsm1Acceleration.class);
        ppgRawTopic = createTopic("android_biovotion_vsm1_ppg_raw",
                BiovotionVsm1PpgRaw.class);
        ledCurrentTopic = createTopic("android_biovotion_vsm1_led_current",
                BiovotionVsm1LedCurrent.class);
    }

    public AvroTopic<ObservationKey, BiovotionVsm1BatteryLevel> getBatteryStateTopic() {
        return batteryStateTopic;
    }

    public AvroTopic<ObservationKey, BiovotionVsm1BloodPulseWave> getBloodPulseWaveTopic() {
        return bloodPulseWaveTopic;
    }

    public AvroTopic<ObservationKey, BiovotionVsm1OxygenSaturation> getSpO2Topic() {
        return spo2Topic;
    }

    public AvroTopic<ObservationKey, BiovotionVsm1HeartRate> getHeartRateTopic() {
        return heartRateTopic;
    }

    public AvroTopic<ObservationKey, BiovotionVsm1HeartRateVariability> getHrvTopic() {
        return hrvTopic;
    }

    public AvroTopic<ObservationKey, BiovotionVsm1RespirationRate> getRespirationRateTopic() {
        return respirationTopic;
    }

    public AvroTopic<ObservationKey, BiovotionVsm1Energy> getEnergyTopic() {
        return energyTopic;
    }

    public AvroTopic<ObservationKey, BiovotionVsm1Temperature> getTemperatureTopic() {
        return temperatureTopic;
    }

    public AvroTopic<ObservationKey, BiovotionVsm1GalvanicSkinResponse> getGsrTopic() {
        return gsrTopic;
    }

    public AvroTopic<ObservationKey, BiovotionVsm1Acceleration> getAccelerationTopic() {
        return accelerationTopic;
    }

    public AvroTopic<ObservationKey, BiovotionVsm1PpgRaw> getPhotoRawTopic() {
        return ppgRawTopic;
    }

    public AvroTopic<ObservationKey, BiovotionVsm1LedCurrent> getLedCurrentTopic() {
        return ledCurrentTopic;
    }
}
