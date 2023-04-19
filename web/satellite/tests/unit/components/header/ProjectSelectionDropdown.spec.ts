// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

import Vuex from 'vuex';

import ProjectSelectionDropdown from '@/components/header/projectSelection/ProjectSelectionDropdown.vue';

import { makeProjectsModule, PROJECTS_MUTATIONS } from '@/store/modules/projects';
import { Project } from '@/types/projects';
import { createLocalVue, shallowMount } from '@vue/test-utils';

import { ProjectsApiMock } from '../../mock/api/projects';

const localVue = createLocalVue();
localVue.use(Vuex);

const projectsApi = new ProjectsApiMock();
const projectsModule = makeProjectsModule(projectsApi);
const project1 = new Project('testId1', 'testName1', '');
const project2 = new Project('testId2', 'testName2', '');

const store = new Vuex.Store({ modules: { projectsModule }});

describe('ProjectSelectionDropdown', () => {
    it('renders correctly', () => {
        store.commit(PROJECTS_MUTATIONS.SET_PROJECTS, [project1, project2]);
        store.commit(PROJECTS_MUTATIONS.SELECT_PROJECT, project1.id);

        const wrapper = shallowMount(ProjectSelectionDropdown, {
            store,
            localVue,
        });

        expect(wrapper).toMatchSnapshot();
    });
});
