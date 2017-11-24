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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Locale;

import ch.hevs.biovotion.vsm.parameters.Parameter;

/** Manages GAP status and requests for a Biovotion VSM wearable, for raw data streaming */
public class BiovotionGAPManager {
    private static final Logger logger = LoggerFactory.getLogger(BiovotionGAPManager.class);

    private String deviceId;
    private int gapStatus;
    private final ByteBuffer gapRequestBuffer;
    private BiovotionGAPState rawGap;


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

    public BiovotionGAPManager(Context context) {
        this.gapRequestBuffer = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN);
        this.gapStatus = -1;
        this.rawGap = new BiovotionGAPState(context);
        this.deviceId = null;
    }


    public int getStatus() {
        return this.gapStatus;
    }
    public void setStatus(int status) {
        this.gapStatus = status;
        this.rawGap.setGapStatus(status);
    }

    public BiovotionGAPState getRawGap() {
        return rawGap;
    }
    public void setRawGap(BiovotionGAPState raw_gap) {
        this.rawGap = raw_gap;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
        this.getRawGap().setDeviceId(deviceId);
    }


    /**
     * make a new GAP request
     * @param gap_type data type of GAP request
     * @param gap_start counter value from where to begin (backwards, e.g. last counter)
     * @param gap_range number of records to get
     * @return Parameter with gap request info
     */
    public Parameter buildGapRequest(int gap_type, int gap_start, int gap_range) {
        if (gap_start <= 0 || gap_range <= 0 || gap_range > gap_start+1) return null;

        // build the complete request value byte array
        byte[] ba_gap_req_value = gapRequestBuffer
                .put((byte) gap_type)
                .putInt(gap_start)
                .putInt(gap_range)
                .array();
        gapRequestBuffer.clear();

        // send the request
        logger.info("Biovotion VSM GAP new request: type:{} start:{} range:{}", gap_type, gap_start, gap_range);
        return Parameter.fromBytes(VsmConstants.PID_GAP_RECORD_REQUEST, ba_gap_req_value);
    }

    public Parameter newRawGap() {
        if (getRawGap().requestViableCheck() && getRawGap().pageSizeCheck()) {
            // TODO: FIXME: sometimes the device seems to send a lot more data than was requested in the last GAP (-> SinceLast > LastRange), need to find out the cause (maybe firmware issue?) (-> duplicate data? unnecessary lag?)
            if (getRawGap().getGapSinceLast() != getRawGap().getGapLastRange())
                logger.warn("Biovotion VSM GAP num samples since last request ({}) is not equal with num records to get ({})!", getRawGap().getGapSinceLast(), getRawGap().getGapLastRange());
            getRawGap().setGapSinceLast(0);
            setStatus(0); // manually override status to prevent immediate second GAP request TODO: do this properly somehow

            int start_ix = getRawGap().nextIndex();
            int records_to_get = getRawGap().recordsToGet();

            return buildGapRequest(VsmConstants.GAP_TYPE_VITAL_RAW, start_ix, records_to_get);
        } else if (getRawGap().requestViableCheck() && !getRawGap().pageSizeCheck()) {
            logger.warn("Biovotion VSM GAP pageSizeCheck:{} nextIndex():{} recordsToGet:{}", getRawGap().pageSizeCheck(), getRawGap().nextIndex(), getRawGap().recordsToGet());
        }

        return null;
    }

    public void rawGapSuccessful() {
        getRawGap().setGapLastRange(getRawGap().recordsToGet());
        getRawGap().setGapLastIndex(getRawGap().nextIndex());
        //getRawGap().setGapLastIndex(getRawGap().getGapCount()); // DEBUG: reset gapLastIndex
    }


    @Override
    public String toString() {
        return String.format(Locale.getDefault(), "Status: %d\nRAW: %s", getStatus(), getRawGap());
    }
}
