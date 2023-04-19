// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

import Vuex from 'vuex';

import ApiKeysArea from '@/components/apiKeys/ApiKeysArea.vue';

import { PaymentsHttpApi } from '@/api/payments';
import { API_KEYS_MUTATIONS, makeApiKeysModule } from '@/store/modules/apiKeys';
import { makeNotificationsModule } from '@/store/modules/notifications';
import { makePaymentsModule } from '@/store/modules/payments';
import { makeProjectsModule } from '@/store/modules/projects';
import { ApiKey, ApiKeysPage } from '@/types/apiKeys';
import { Project } from '@/types/projects';
import { NotificatorPlugin } from '@/utils/plugins/notificator';
import { SegmentioPlugin } from '@/utils/plugins/segment';
import { createLocalVue, mount, shallowMount } from '@vue/test-utils';

import { ApiKeysMock } from '../mock/api/apiKeys';
import { ProjectsApiMock } from '../mock/api/projects';

const localVue = createLocalVue();
const segmentioPlugin = new SegmentioPlugin();
const notificationPlugin = new NotificatorPlugin();
localVue.use(Vuex);
localVue.use(segmentioPlugin);
localVue.use(notificationPlugin);

const apiKeysApi = new ApiKeysMock();
const apiKeysModule = makeApiKeysModule(apiKeysApi);
const projectsApi = new ProjectsApiMock();
const projectsModule = makeProjectsModule(projectsApi);
const paymentsApi = new PaymentsHttpApi();
const paymentsModule = makePaymentsModule(paymentsApi);
const notificationsModule = makeNotificationsModule();
const { CLEAR, SET_PAGE } = API_KEYS_MUTATIONS;
const store = new Vuex.Store({ modules: { projectsModule, apiKeysModule, paymentsModule, notificationsModule }});

describe('ApiKeysArea', () => {
    const project = new Project('id', 'projectName', 'projectDescription', 'test', 'testOwnerId', true);
    projectsApi.setMockProjects([project]);

    const apiKey = new ApiKey('testId', 'test', 'test', 'test');
    const apiKey1 = new ApiKey('testId1', 'test1', 'test1', 'test1');

    const testApiKeysPage = new ApiKeysPage();
    testApiKeysPage.apiKeys = [apiKey];
    testApiKeysPage.totalCount = 1;
    testApiKeysPage.pageCount = 1;

    apiKeysApi.setMockApiKeysPage(testApiKeysPage);

    it('renders correctly', () => {
        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        expect(wrapper).toMatchSnapshot();
    });

    it('function apiKeyList works correctly', () => {
        store.commit(SET_PAGE, testApiKeysPage);

        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        expect(wrapper.vm.apiKeyList).toEqual([apiKey]);
    });

    it('action on toggleSelection works correctly', async () => {
        store.commit(SET_PAGE, testApiKeysPage);

        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        await wrapper.vm.toggleSelection(apiKey);

        expect(store.getters.selectedApiKeys.length).toBe(1);
    });

    it('action on onClearSelection works correctly', async () => {
        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        await wrapper.vm.onClearSelection();

        expect(wrapper.vm.$data.isDeleteClicked).toBe(false);
    });

    it('function onCreateApiKeyClick works correctly', async () => {
        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        await wrapper.vm.onCreateApiKeyClick();

        expect(wrapper.vm.$data.isNewApiKeyPopupShown).toBe(true);
    });

    it('function onFirstDeleteClick works correctly', async () => {
        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        await wrapper.vm.onFirstDeleteClick();

        expect(wrapper.vm.$data.isDeleteClicked).toBe(true);
    });

    it('function apiKeyCountTitle works correctly', () => {
        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        expect(wrapper.vm.apiKeyCountTitle).toMatch('api key');
    });

    it('function isEmpty works correctly', () => {
        store.commit(SET_PAGE, testApiKeysPage);

        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        expect(wrapper.vm.isEmpty).toBe(false);
    });

    it('function selectedAPIKeysCount works correctly', () => {
        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        expect(wrapper.vm.selectedAPIKeysCount).toBe(0);
    });

    it('function headerState works correctly', () => {
        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        expect(wrapper.vm.headerState).toBe(0);
    });

    it('function apiKeyCountTitle with 2 keys works correctly', () => {
        const testPage = new ApiKeysPage();
        testPage.apiKeys = [apiKey, apiKey1];
        testPage.totalCount = 1;
        testPage.pageCount = 1;

        apiKeysApi.setMockApiKeysPage(testPage);

        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        expect(wrapper.vm.apiKeyCountTitle).toMatch('api keys');
    });

    it('function closeNewApiKeyPopup works correctly', async () => {
        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        await wrapper.vm.closeNewApiKeyPopup();

        expect(wrapper.vm.$data.isNewApiKeyPopupShown).toBe(false);
    });

    it('function showCopyApiKeyPopup works correctly', async () => {
        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        const testSecret = 'testSecret';

        await wrapper.vm.showCopyApiKeyPopup(testSecret);

        expect(wrapper.vm.$data.isCopyApiKeyPopupShown).toBe(true);
        expect(wrapper.vm.$data.apiKeySecret).toMatch('testSecret');
    });

    it('function closeCopyNewApiKeyPopup works correctly', async () => {
        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        await wrapper.vm.closeCopyNewApiKeyPopup();

        expect(wrapper.vm.$data.isCopyApiKeyPopupShown).toBe(false);
    });

    it('action on onDelete with name works correctly', async () => {
        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        await wrapper.vm.onDelete();

        expect(wrapper.vm.$data.isDeleteClicked).toBe(false);
    });

    it('renders empty screen with add key prompt', () => {
        store.commit(CLEAR);

        const wrapper = mount(ApiKeysArea, {
            store,
            localVue,
        });

        expect(wrapper).toMatchSnapshot();
    });

    it('renders empty search state correctly', () => {
        const testPage = new ApiKeysPage();
        testPage.apiKeys = [];
        testPage.totalCount = 0;
        testPage.pageCount = 0;
        testPage.search = 'testSearch';
        apiKeysApi.setMockApiKeysPage(testPage);

        store.commit(SET_PAGE, testPage);

        const wrapper = shallowMount(ApiKeysArea, {
            store,
            localVue,
        });

        expect(wrapper).toMatchSnapshot();
    });
});
