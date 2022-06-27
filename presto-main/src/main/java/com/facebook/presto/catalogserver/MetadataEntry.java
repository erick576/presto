/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.catalogserver;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;

import java.util.Objects;

@ThriftStruct
public class MetadataEntry<T>
{
    private final T value;
    private final boolean isCacheHit;

    @ThriftConstructor
    public MetadataEntry(T value, boolean isCacheHit)
    {
        this.value = value;
        this.isCacheHit = isCacheHit;
    }

    @ThriftField(1)
    public T getValue()
    {
        return value;
    }

    @ThriftField(2)
    public boolean getIsCacheHit()
    {
        return isCacheHit;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetadataEntry metadataKey = (MetadataEntry) o;
        return Objects.equals(value, metadataKey.getValue());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }
}
