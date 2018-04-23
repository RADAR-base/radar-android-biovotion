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

import org.apache.avro.specific.SpecificRecord;
import org.radarcns.android.device.DeviceService;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.topic.AvroTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A service that manages a BiovotionDeviceManager and a TableDataHandler to send store the data of a
 * Biovotion VSM and send it to a Kafka REST proxy.
 */
public class BiovotionService extends DeviceService<BiovotionDeviceStatus> {
    private static final Logger logger = LoggerFactory.getLogger(BiovotionService.class);
    private Map<String, Long> deviceBlacklist = Collections.synchronizedMap(new HashMap<String, Long>());

    @Override
    protected BiovotionDeviceManager createDeviceManager() {
        return new BiovotionDeviceManager(this);
    }

    @Override
    protected BiovotionDeviceStatus getDefaultState() {
        return new BiovotionDeviceStatus();
    }

    public Map<String, Long> getDeviceBlacklist() {
        return deviceBlacklist;
    }

    public void setDeviceBlacklist(Map<String, Long> deviceBlacklist) {
        this.deviceBlacklist = deviceBlacklist;
    }
}
