package io.ballerina.lib.aws.redshiftdata;

import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.math.BigDecimal;

public record ResultConfig(BigDecimal timeout, BigDecimal pollingInterval) {

    public ResultConfig(BMap<BString, Object> bResultConfig) {
        this(getTimeout(bResultConfig), getPollingInterval(bResultConfig));
    }

    private static BigDecimal getTimeout(BMap<BString, Object> bResultConfig) {
        BDecimal bTimeout = (BDecimal) bResultConfig.get(Constants.TIMEOUT);
        return bTimeout.decimalValue();
    }

    private static BigDecimal getPollingInterval(BMap<BString, Object> bResultConfig) {
        BDecimal bPollingInterval = (BDecimal) bResultConfig.get(Constants.POLLING_INTERVAL);
        return bPollingInterval.decimalValue();
    }
}
