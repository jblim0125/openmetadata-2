/*
 *  Copyright 2022 Collate.
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

import { AxiosResponse } from 'axios';
import { Operation } from 'fast-json-patch';
import { PagingResponse, RestoreRequestType } from 'Models';
import { APPLICATION_JSON_CONTENT_TYPE_HEADER } from '../constants/constants';
import { SystemProfile } from '../generated/api/data/createTableProfile';
import {
  ColumnProfile,
  TableProfile,
  TableProfilerConfig,
} from '../generated/entity/data/table';
import { Container } from '../generated/entity/data/container';
// import { EntityHistory } from '../generated/type/entityHistory';
import { EntityReference } from '../generated/type/entityReference';
import { Include } from '../generated/type/include';
import { Paging } from '../generated/type/paging';
import { ListParams } from '../interface/API.interface';
import { getEncodedFqn } from '../utils/StringsUtils';
import APIClient from './index';

const BASE_URL = '/containers';

// export const getContainerVersions = async (id: string) => {
//   const url = `${BASE_URL}/${id}/versions`;
//
//   const response = await APIClient.get<EntityHistory>(url);
//
//   return response.data;
// };
//
// export const getContainerVersion = async (id: string, version: string) => {
//   const url = `${BASE_URL}/${id}/versions/${version}`;
//
//   const response = await APIClient.get(url);
//
//   return response.data;
// };

export const getContainerDetailsByFQN = async (
  fqn: string,
  params?: ListParams
) => {
  const response = await APIClient.get<Container>(
    `${BASE_URL}/name/${getEncodedFqn(fqn)}`,
    {
      params: { ...params, include: params?.include ?? Include.All },
    }
  );

  return response.data;
};

export const patchContainerDetails = async (id: string, data: Operation[]) => {
  const response = await APIClient.patch<Operation[], AxiosResponse<Container>>(
    `${BASE_URL}/${id}`,
    data
  );

  return response.data;
};

export const restoreContainer = async (id: string) => {
  const response = await APIClient.put<
    RestoreRequestType,
    AxiosResponse<Container>
  >(`${BASE_URL}/restore`, { id });

  return response.data;
};

export const addFollower = async (containerId: string, userId: string) => {
  const response = await APIClient.put<
    string,
    AxiosResponse<{
      changeDescription: { fieldsAdded: { newValue: EntityReference[] }[] };
    }>
  >(
    `${BASE_URL}/${containerId}/followers`,
    userId,
    APPLICATION_JSON_CONTENT_TYPE_HEADER
  );

  return response.data;
};

export const removeFollower = async (containerId: string, userId: string) => {
  const response = await APIClient.delete<
    string,
    AxiosResponse<{
      changeDescription: { fieldsDeleted: { oldValue: EntityReference[] }[] };
    }>
  >(
    `${BASE_URL}/${containerId}/followers/${userId}`,
    APPLICATION_JSON_CONTENT_TYPE_HEADER
  );

  return response.data;
};

export const getTableProfilerConfig = async (containerId: string) => {
  const response = await APIClient.get<Container>(
    `${BASE_URL}/${containerId}/tableProfilerConfig`
  );

  return response.data;
};

export const putTableProfileConfig = async (
  containerId: string,
  data: TableProfilerConfig
) => {
  const response = await APIClient.put<
    TableProfilerConfig,
    AxiosResponse<Container>
  >(
    `${BASE_URL}/${containerId}/tableProfilerConfig`,
    data,
    APPLICATION_JSON_CONTENT_TYPE_HEADER
  );

  return response.data;
};

export const getTableProfilesList = async (
  containerFqn: string,
  params?: {
    startTs?: number;
    endTs?: number;
  }
) => {
  const url = `${BASE_URL}/${getEncodedFqn(containerFqn)}/tableProfile`;

  const response = await APIClient.get<PagingResponse<TableProfile[]>>(url, {
    params,
  });

  return response.data;
};

export const getSystemProfileList = async (
  containerFqn: string,
  params?: {
    startTs?: number;
    endTs?: number;
  }
) => {
  const url = `${BASE_URL}/${getEncodedFqn(containerFqn)}/systemProfile`;

  const response = await APIClient.get<PagingResponse<SystemProfile[]>>(url, {
    params,
  });

  return response.data;
};

export const getColumnProfilerList = async (
  columnFqn: string,
  params?: {
    startTs?: number;
    endTs?: number;
    limit?: number;
    before?: string;
    after?: string;
  }
) => {
  const url = `${BASE_URL}/${getEncodedFqn(columnFqn)}/columnProfile`;

  const response = await APIClient.get<{
    data: ColumnProfile[];
    paging: Paging;
  }>(url, { params });

  return response.data;
};

export const getSampleDataByContainerId = async (id: string) => {
  const response = await APIClient.get<Container>(
    `${BASE_URL}/${id}/sampleData`
  );

  return response.data;
};

export const getLatestContainerProfileByFqn = async (fqn: string) => {
  const encodedFQN = getEncodedFqn(fqn);
  const response = await APIClient.get<Container>(
    `${BASE_URL}/${encodedFQN}/tableProfile/latest`
  );

  return response.data;
};

export const deleteSampleDataByContainerId = async (id: string) => {
  return await APIClient.delete<Container>(`${BASE_URL}/${id}/sampleData`);
};
