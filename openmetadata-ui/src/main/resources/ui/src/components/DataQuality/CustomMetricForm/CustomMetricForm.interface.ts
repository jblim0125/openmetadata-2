/*
 *  Copyright 2023 Collate.
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
import { FormInstance } from 'antd';
import { Table } from '../../../generated/entity/data/table';
import { Container } from '../../../generated/entity/data/container';
import { CustomMetric } from '../../../generated/tests/customMetric';

export interface ContainerCustomMetricFormProps {
  isColumnMetric: boolean;
  initialValues?: CustomMetric;
  onFinish: (values: CustomMetric) => void;
  form?: FormInstance<CustomMetric>;
  container?: Container;
  isEditMode?: boolean;
}

export interface CustomMetricFormProps {
  isColumnMetric: boolean;
  initialValues?: CustomMetric;
  onFinish: (values: CustomMetric) => void;
  form?: FormInstance<CustomMetric>;
  table?: Table;
  isEditMode?: boolean;
}
