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

import android.os.Parcel;
import android.os.Parcelable;

import org.radarcns.android.device.BaseDeviceState;
import org.radarcns.android.device.DeviceStateCreator;

/**
 * The status on a single point in time of a Biovotion VSM device.
 */
public class BiovotionDeviceStatus extends BaseDeviceState {
    private float[] battery = {Float.NaN, Float.NaN, Float.NaN, Float.NaN};

    private float[] bloodPulseWave = {Float.NaN, Float.NaN};

    private float[] spo2 = {Float.NaN, Float.NaN};

    private float[] heartRate = {Float.NaN, Float.NaN};

    private float[] heartRateVariability = {Float.NaN, Float.NaN};

    private float[] respirationRate = {Float.NaN, Float.NaN};

    private float[] energy = {Float.NaN, Float.NaN};

    private float[] temperature = {Float.NaN, Float.NaN, Float.NaN};

    private float[] galvanicSkinResponse = {Float.NaN, Float.NaN};

    private float[] acceleration = {Float.NaN, Float.NaN, Float.NaN};

    private float[] ppgRaw = {Float.NaN, Float.NaN, Float.NaN, Float.NaN};

    private float[] ledCurrent = {Float.NaN, Float.NaN, Float.NaN, Float.NaN};

    public static final Parcelable.Creator<BiovotionDeviceStatus> CREATOR = new DeviceStateCreator<>(BiovotionDeviceStatus.class);

    @Override
    public synchronized void writeToParcel(Parcel dest, int flags) {
        super.writeToParcel(dest, flags);
        dest.writeFloat(this.battery[0]);
        dest.writeFloat(this.battery[1]);
        dest.writeFloat(this.battery[2]);
        dest.writeFloat(this.battery[3]);
        dest.writeFloat(this.bloodPulseWave[0]);
        dest.writeFloat(this.bloodPulseWave[1]);
        dest.writeFloat(this.spo2[0]);
        dest.writeFloat(this.spo2[1]);
        dest.writeFloat(this.heartRate[0]);
        dest.writeFloat(this.heartRate[1]);
        dest.writeFloat(this.heartRateVariability[0]);
        dest.writeFloat(this.heartRateVariability[1]);
        dest.writeFloat(this.respirationRate[0]);
        dest.writeFloat(this.respirationRate[1]);
        dest.writeFloat(this.energy[0]);
        dest.writeFloat(this.energy[1]);
        dest.writeFloat(this.temperature[0]);
        dest.writeFloat(this.temperature[1]);
        dest.writeFloat(this.temperature[2]);
        dest.writeFloat(this.galvanicSkinResponse[0]);
        dest.writeFloat(this.galvanicSkinResponse[1]);
        dest.writeFloat(this.acceleration[0]);
        dest.writeFloat(this.acceleration[1]);
        dest.writeFloat(this.acceleration[2]);
        dest.writeFloat(this.ppgRaw[0]);
        dest.writeFloat(this.ppgRaw[1]);
        dest.writeFloat(this.ppgRaw[2]);
        dest.writeFloat(this.ppgRaw[3]);
        dest.writeFloat(this.ledCurrent[0]);
        dest.writeFloat(this.ledCurrent[1]);
        dest.writeFloat(this.ledCurrent[2]);
        dest.writeFloat(this.ledCurrent[3]);
    }

    public void updateFromParcel(Parcel in) {
        super.updateFromParcel(in);
        battery[0] = in.readFloat();
        battery[1] = in.readFloat();
        battery[2] = in.readFloat();
        battery[3] = in.readFloat();
        bloodPulseWave[0] = in.readFloat();
        bloodPulseWave[1] = in.readFloat();
        spo2[0] = in.readFloat();
        spo2[1] = in.readFloat();
        heartRate[0] = in.readFloat();
        heartRate[1] = in.readFloat();
        heartRateVariability[0] = in.readFloat();
        heartRateVariability[1] = in.readFloat();
        respirationRate[0] = in.readFloat();
        respirationRate[1] = in.readFloat();
        energy[0] = in.readFloat();
        energy[1] = in.readFloat();
        temperature[0] = in.readFloat();
        temperature[1] = in.readFloat();
        temperature[2] = in.readFloat();
        galvanicSkinResponse[0] = in.readFloat();
        galvanicSkinResponse[1] = in.readFloat();
        acceleration[0] = in.readFloat();
        acceleration[1] = in.readFloat();
        acceleration[2] = in.readFloat();
        ppgRaw[0] = in.readFloat();
        ppgRaw[1] = in.readFloat();
        ppgRaw[2] = in.readFloat();
        ppgRaw[3] = in.readFloat();
        ledCurrent[0] = in.readFloat();
        ledCurrent[1] = in.readFloat();
        ledCurrent[2] = in.readFloat();
        ledCurrent[3] = in.readFloat();
    }


    /**
     * getter
     */

    public float getBatteryLevel() { return battery[0]; }
    public float[] getBattery() {
        return battery;
    }

    public float[] getBloodPulseWave() { return bloodPulseWave; }

    public float[] getSpO2() { return spo2; }

    @Override
    public boolean hasHeartRate() { return true; }
    public float getHeartRate() { return heartRate[0]; }
    public float[] getHeartRateAll() { return heartRate; }

    public float[] getHeartRateVariability() { return heartRateVariability; }

    public float[] getRespirationRate() { return respirationRate; }

    public float[] getEnergy() { return energy; }

    @Override
    public boolean hasTemperature() { return true; }
    public float getTemperature() { return temperature[0]; }
    public float[] getTemperatureAll() { return temperature; }

    public float[] getGalvanicSkinResponse() { return galvanicSkinResponse; }

    @Override
    public boolean hasAcceleration() {
        return true;
    }
    public float[] getAcceleration() {
        return acceleration;
    }

    public float[] getPhotoRaw() {
        return ppgRaw;
    }

    public float[] getLedCurrent() {
        return ledCurrent;
    }



    /*
     * setters
     */

    public void setBattery(float level, float rate, float voltage, float status) {
        this.battery[0] = level / 100.0f;
        this.battery[1] = rate / 100.0f;
        this.battery[2] = voltage / 10.0f;
        this.battery[3] = status;
    }

    public void setBloodPulseWave(float value, float quality) {
        this.bloodPulseWave[0] = value / 50.0f;
        this.bloodPulseWave[1] = quality / 100.0f;
    }

    public void setSpo2(float value, float quality) {
        this.spo2[0] = value / 100.0f;
        this.spo2[1] = quality / 100.0f;
    }

    public void setHeartRate(float value, float quality) {
        this.heartRate[0] = value;
        this.heartRate[1] = quality / 100.0f;
    }

    public void setHeartRateVariability(float value, float quality) {
        this.heartRateVariability[0] = value;
        this.heartRateVariability[1] = quality / 100.0f;
    }

    public void setRespirationRate(float value, float quality) {
        this.respirationRate[0] = value;
        this.respirationRate[1] = quality / 100.0f;
    }

    public void setEnergy(float value, float quality) {
        this.energy[0] = value * 2.0f;
        this.energy[1] = quality / 100.0f;
    }

    public void setTemperature(float temp, float tempLocal, float tempBaro) {
        this.temperature[0] = temp / 100.0f;
        this.temperature[1] = tempLocal / 100.0f;
        this.temperature[2] = tempBaro / 100.0f;
    }

    public void setGalvanicSkinResponse(float amp, float phase) {
        this.galvanicSkinResponse[0] = amp / 3000.0f;
        this.galvanicSkinResponse[1] = phase;
    }

    public void setAcceleration(float x, float y, float z) {
        this.acceleration[0] = x / (float)Math.pow(2, 12);
        this.acceleration[1] = y / (float)Math.pow(2, 12);
        this.acceleration[2] = z / (float)Math.pow(2, 12);
    }

    public void setPhotoRaw(float red, float green, float ir, float dark) {
        this.ppgRaw[0] = red;
        this.ppgRaw[1] = green;
        this.ppgRaw[2] = ir;
        this.ppgRaw[3] = dark;
    }

    public void setLedCurrent(float red, float green, float ir, float offset) {
        this.ledCurrent[0] = red;
        this.ledCurrent[1] = green;
        this.ledCurrent[2] = ir;
        this.ledCurrent[3] = offset;
    }
}
