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
import android.content.SharedPreferences;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.hevs.biovotion.vsm.parameters.Parameter;

/** Manages the state of a GAP request lifetime */
public class BiovotionGAPState {
    private static final Logger logger = LoggerFactory.getLogger(BiovotionGAPState.class);

    private int gapStatus;        // current GAP status
    private int gapResponse;      // last GAP request response

    private int gapCount;         // current latest counter index
    private int gapNum;           // current total number of records in storage
    private int gapLastIndex;     // start index from last GAP request
    private int gapLastGet;       // number of records requested from last GAP request
    private int gapSinceLast;     // number of records streamed since last GAP request
    private int gapStreamLag;     // approximate number of records the streaming lags behind the device

    private Parameter lastGapRequest; // parameter set of last sent GAP request
    private String deviceId;
    private Context context;

    public BiovotionGAPState(Context context) {
        this.gapStatus = -1;
        this.gapResponse = 0;
        this.gapLastIndex = -1;
        this.gapSinceLast = 0;
        this.gapLastGet = 0;
        this.gapCount = -1;
        this.gapNum = -1;

        this.lastGapRequest = null;
        this.deviceId = null;
        this.context = context;
    }

    @Override
    public String toString() {
        return "BiovotionGAPState{" +
                "gapStatus=" + gapStatus +
                ", gapResponse=" + gapResponse +
                ", gapCount=" + gapCount +
                ", gapNum=" + gapNum +
                ", gapLastIndex=" + gapLastIndex +
                ", gapLastGet=" + gapLastGet +
                ", gapSinceLast=" + gapSinceLast +
                ", gapStreamLag=" + gapStreamLag +
                '}';
    }

    public boolean pageSizeCheck() {
        boolean index_multiple = (nextIndex()+1) % VsmConstants.GAP_MAX_PER_PAGE_VITAL_RAW == 0;    // dont make a request if the index is not a multiple of the page size, to avoid getting more records than were requested
        boolean toget_multiple = recordsToGet() % VsmConstants.GAP_MAX_PER_PAGE_VITAL_RAW == 0;     // dont make a request if the number of records to get is not a multiple of the page size, to avoid getting more records than were requested
        return index_multiple && toget_multiple;
    }
    public boolean requestViableCheck() {
        return getGapStatus() != 0 && getGapCount() > 0 && getGapNum() > 0 && recordsToGet() > 0
                && (getGapNum() - recordsToGet()) > 0; // cant request more records than are stored on the device
    }

    public int nextIndex() {
        return getGapLastIndex() + recordsToGet();
    }

    public int recordsToGet() {
        int records_to_get = getGapCount() - getGapLastIndex();
        if (records_to_get > VsmConstants.GAP_MAX_PAGES * VsmConstants.GAP_MAX_PER_PAGE_VITAL_RAW)
            records_to_get = VsmConstants.GAP_MAX_PAGES * VsmConstants.GAP_MAX_PER_PAGE_VITAL_RAW;
        return records_to_get;
    }

    public void clearPrefs() {
        SharedPreferences prefs = this.context.getSharedPreferences(VsmConstants.BIOVOTION_PREFS, Context.MODE_PRIVATE);
        prefs.edit().clear().apply();
    }

    public int samples_from_ms(int ms) {
        int samples = (int) Math.ceil(VsmConstants.VSM_RAW_SAMPLE_RATE * ms/1000);
        samples = (samples + VsmConstants.GAP_MAX_PER_PAGE_VITAL_RAW) - (samples % VsmConstants.GAP_MAX_PER_PAGE_VITAL_RAW); // closest integer divisible by page size greater than samples
        return samples;
    }


    /*
     * Getter/Setter
     */

    public int getGapStatus() {
        return gapStatus;
    }

    public void setGapStatus(int gapStatus) {
        this.gapStatus = gapStatus;
    }

    public int getGapResponse() {
        return gapResponse;
    }

    public void setGapResponse(int gapResponse) {
        this.gapResponse = gapResponse;
    }

    public int getGapCount() {
        return gapCount;
    }

    public void setGapCount(int gapCount) {
        this.gapCount = gapCount;
        updateGapStreamLag(); // update streaming lag behind device
    }

    public int getGapNum() {
        return gapNum;
    }

    public void setGapNum(int gapNum) {
        this.gapNum = gapNum;
    }

    public int getGapLastIndex() {
        if (getDeviceId() != null && gapLastIndex < 0) {
            SharedPreferences prefs = this.context.getSharedPreferences(VsmConstants.BIOVOTION_PREFS, Context.MODE_PRIVATE);
            gapLastIndex = prefs.getInt(VsmConstants.GAP_LAST_INDEX+"_"+getDeviceId(), -1);
        }
        if (getGapCount() - gapLastIndex > samples_from_ms(VsmConstants.GAP_MAX_LOOKBACK_MS))
            gapLastIndex = getGapCount() - samples_from_ms(VsmConstants.GAP_MAX_LOOKBACK_MS);
        return gapLastIndex;
    }

    public void setGapLastIndex(int gapLastIndex) {
        if (getDeviceId() != null) {
            SharedPreferences prefs = this.context.getSharedPreferences(VsmConstants.BIOVOTION_PREFS, Context.MODE_PRIVATE);
            prefs.edit().putInt(VsmConstants.GAP_LAST_INDEX+"_"+getDeviceId(), gapLastIndex).apply();
        }
        this.gapLastIndex = gapLastIndex;
    }

    public int getGapLastGet() {
        return gapLastGet;
    }

    public void setGapLastGet(int gapLastGet) {
        this.gapLastGet = gapLastGet;
    }

    public int getGapSinceLast() {
        return gapSinceLast;
    }

    public void setGapSinceLast(int gapSinceLast) {
        this.gapSinceLast = gapSinceLast;
    }

    public void incGapSinceLast() {
        this.gapSinceLast++;
    }

    public int getGapStreamLag() {
        updateGapStreamLag();
        return gapStreamLag;
    }

    public void setGapStreamLag(int gapStreamLag) {
        this.gapStreamLag = gapStreamLag;
    }

    public void updateGapStreamLag() {
        setGapStreamLag(getGapCount() - getGapLastIndex());
    }

    public Parameter getLastGapRequest() {
        return lastGapRequest;
    }

    public void setLastGapRequest(Parameter lastGapRequest) {
        this.lastGapRequest = lastGapRequest;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }
}
