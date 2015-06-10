
import datetime
import hashlib
import logging
import urllib2
import uuid
import re

from pylons import config

from ckanext.clearinghouse.model.clearinghouse_metadata import ClearinghouseXmlObject, ClearinghouseDocument
from ckan.logic import NotFound, get_action

from ckan import logic
from ckan import model
from ckan import plugins as p
from ckan.model import Session

from ckan.plugins.core import SingletonPlugin, implements

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra

from ckan.lib.helpers import json
from ckan.lib.navl.validators import not_empty


log = logging.getLogger(__name__)


class ClearinghouseHarvester(HarvesterBase, SingletonPlugin):
    '''
    A Harvester for CCCCC ClearingHouse
    '''
    implements(IHarvester)

    _user_name = None

    source_config = {}

    def info(self):
        return {
            'name': 'clearinghouse',
            'title': 'CCCCC ClearingHouse',
            'description': 'Harvests CCCCC ClearingHouse',
            'form_config_interface': 'Text'
        }

    ## IHarvester

    def validate_config(self, source_config):
        if not source_config:
            return source_config

        try:
            source_config_obj = json.loads(source_config)

            if 'topic_group_mapping' in source_config_obj:
                mapping = source_config_obj['topic_group_mapping']
                if not isinstance(mapping, dict):
                    raise ValueError('topic_group_mapping should be a dict (it maps group names to list of regex)')
                for k, v in mapping.iteritems():
                    if not isinstance(k, basestring):
                        raise ValueError('topic_group_mapping keys should be strings (it maps group names to list of regex)')
                    if not isinstance(v, list):
                        raise ValueError('topic_group_mapping values should be lists (it maps group names to list of regex)')

        except ValueError as e:
            raise e

        return source_config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.clearinghouse.gather')
        log.debug('ClearingHouseHarvester gather_stage for job: %r', harvest_job)
        # Get source URL
        url = harvest_job.source.url

        self._set_source_config(harvest_job.source.config)

        try:
            log.info('Connecting to ClearingHouse at %s', url)
            request = urllib2.Request(url)
            opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(), urllib2.HTTPRedirectHandler())

            response = opener.open(request)
            content = response.read()

            #logger.info('----> %s', content)
            #print 'RESPONSE ', content

            chobj = ClearinghouseXmlObject(content)

        except Exception as e:
            self._save_gather_error('Error harvesting ClearingHouse: %s' % e, harvest_job)
            return None

        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id).\
                                    filter(HarvestObject.current == True).\
                                    filter(HarvestObject.harvest_source_id == harvest_job.source.id)
        guid_to_package_id = {}

        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = set(guid_to_package_id.keys())

        #log.debug('Starting gathering for %s' % url)
        guids_in_harvest = set(chobj.docs.keys())

        #for doc in chobj.docs:
            #doc_id = doc.get_id()
            #log.info("Got id from ClearingHouse %s", doc_id)
            #guids_in_harvest.add(doc_id)

        new = guids_in_harvest - guids_in_db
        delete = guids_in_db - guids_in_harvest
        change = guids_in_db & guids_in_harvest

        ids = []
        for guid in new:
            doc = chobj.docs[guid].tostring()
            obj = HarvestObject(guid=guid, job=harvest_job, content=doc,
                                extras=[HOExtra(key='status', value='new')])
            obj.save()
            ids.append(obj.id)
        for guid in change:
            doc = chobj.docs[guid].tostring()
            obj = HarvestObject(guid=guid, job=harvest_job, content=doc,
                                package_id=guid_to_package_id[guid],
                                extras=[HOExtra(key='status', value='change')])
            obj.save()
            ids.append(obj.id)
        for guid in delete:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HOExtra(key='status', value='delete')])
            ids.append(obj.id)
            model.Session.query(HarvestObject).\
                  filter_by(guid=guid).\
                  update({'current': False}, False)
            obj.save()

        if len(ids) == 0:
            self._save_gather_error('No records received from the ClearingHouse', harvest_job)
            return None

        return ids

    def fetch_stage(self, harvest_object):
        '''Not really useful: contents is retrieved all at once in gather stage'''

        log = logging.getLogger(__name__ + '.clearinghouse.fetch')
        log.debug('ClearingHouseHarvester fetch_stage for object: %s', harvest_object.id)

        return True

    def import_stage(self, harvest_object):

        log = logging.getLogger(__name__ + '.import')
        log.debug('Import stage for harvest object: %s', harvest_object.id)

        if not harvest_object:
            log.error('No harvest object received')
            return False

        self._set_source_config(harvest_object.source.config)

        status = self._get_object_extra(harvest_object, 'status')

        # Get the last harvested object (if any)
        previous_object = Session.query(HarvestObject) \
                          .filter(HarvestObject.guid == harvest_object.guid) \
                          .filter(HarvestObject.current == True) \
                          .first()

        if status == 'delete':
            # Delete package
            context = {'model': model, 'session': model.Session, 'user': self._get_user_name()}

            p.toolkit.get_action('package_delete')(context, {'id': harvest_object.package_id})
            log.info('Deleted package {0} with guid {1}'.format(harvest_object.package_id, harvest_object.guid))

            return True

        # Parse ISO document
        ##try:
            ##iso_values = ISODocument(harvest_object.content).read_values()
        ##except Exception, e:
            ##self._save_object_error('Error parsing ISO document for object {0}: {1}'.format(harvest_object.id, str(e)),
                                    ##harvest_object, 'Import')
            ##return False

        # Flag previous object as not current anymore
        if previous_object:
            previous_object.current = False
            previous_object.add()

        # Generate GUID if not present (i.e. it's a manual import)
        if not harvest_object.guid:
            self._save_object_error('Missing GUID for object {0}'
                        .format(harvest_object.id), harvest_object, 'Import')
            return False

        # Get document modified date
        #try:
            #metadata_modified_date = dateutil.parser.parse(iso_values['metadata-date'], ignoretz=True)
        #except ValueError:
            #self._save_object_error('Could not extract reference date for object {0} ({1})'
                        #.format(harvest_object.id, iso_values['metadata-date']), harvest_object, 'Import')
            #return False

        #harvest_object.metadata_modified_date = metadata_modified_date
        harvest_object.metadata_modified_date = datetime.date.today()
        harvest_object.add()

        # Build the package dict
        package_dict = self.get_package_dict(harvest_object)
        if not package_dict:
            log.error('No package dict returned, aborting import for object {0}'.format(harvest_object.id))
            return False

        # Create / update the package

        context = {'model': model,
                   'session': model.Session,
                   'user': self._get_user_name(),
                   'extras_as_string': True,
                   'api_version': '2',
                   'return_id_only': True}
        if context['user'] == self._site_user['name']:
            context['ignore_auth'] = True

        # The default package schema does not like Upper case tags
        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty, unicode]

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        if status == 'new':
            package_schema = logic.schema.default_create_package_schema()
            package_schema['tags'] = tag_schema
            context['schema'] = package_schema

            # We need to explicitly provide a package ID, otherwise ckanext-spatial
            # won't be be able to link the extent to the package.
            package_dict['id'] = unicode(uuid.uuid4())
            package_schema['id'] = [unicode]

            # Save reference to the package on the object
            harvest_object.package_id = package_dict['id']
            harvest_object.add()
            # Defer constraints and flush so the dataset can be indexed with
            # the harvest object id (on the after_show hook from the harvester
            # plugin)
            Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
            model.Session.flush()

            try:
                package_id = p.toolkit.get_action('package_create')(context, package_dict)
                log.info('Created new package %s with guid %s', package_id, harvest_object.guid)
            except p.toolkit.ValidationError as e:
                self._save_object_error('Validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                return False

        elif status == 'change':

            # Check if the document has changed
            m = hashlib.md5()
            m.update(previous_object.content)
            old_md5 = m.hexdigest()

            m = hashlib.md5()
            m.update(harvest_object.content)
            new_md5 = m.hexdigest()

            if old_md5 == new_md5:

                # Assign the previous job id to the new object to
                # avoid losing history
                harvest_object.harvest_job_id = previous_object.job.id
                harvest_object.add()

                harvest_object.metadata_modified_date = previous_object.metadata_modified_date

                # Delete the previous object to avoid cluttering the object table
                previous_object.delete()

                log.info('Document with GUID %s unchanged, skipping...' % (harvest_object.guid))
            else:
                package_schema = logic.schema.default_update_package_schema()
                package_schema['tags'] = tag_schema
                context['schema'] = package_schema

                package_dict['id'] = harvest_object.package_id
                try:
                    package_id = p.toolkit.get_action('package_update')(context, package_dict)
                    log.info('Updated package %s with guid %s', package_id, harvest_object.guid)
                except p.toolkit.ValidationError as e:
                    self._save_object_error('Validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                    return False

        model.Session.commit()

        return True

    '''
    These methods can be safely overridden by classes extending
    SpatialHarvester
    '''

    def get_package_dict(self, harvest_object):
        '''
        Constructs a package_dict suitable to be passed to package_create or
        package_update. See documentation on
        ckan.logic.action.create.package_create for more details

        If a dict is not returned by this function, the import stage will be cancelled.

        :param harvest_object: HarvestObject domain object (with access to job and source objects)
        :type harvest_object: HarvestObject

        :returns: A dataset dictionary (package_dict)
        :rtype: dict
        '''

        doc = ClearinghouseDocument(str=harvest_object.content)

        # Extract tags from keywords

        tags = []
        for tag in doc.get_keywords():
            tag = tag[:50] if len(tag) > 50 else tag
            tags.append({'name': tag})

        # Extract groups from topics

        groups = self.handle_groups(harvest_object, doc)

        # Build initial dict

        package_dict = {
            'title': doc.get_title(),
            'notes': doc.get_abstract(),
            'tags': tags,
            'resources': [],
            'groups': groups,
        }

        # We need to get the owner organization (if any) from the harvest
        # source dataset
        source_dataset = model.Package.get(harvest_object.source.id)
        if source_dataset.owner_org:
            package_dict['owner_org'] = source_dataset.owner_org

        # Package name
        package = harvest_object.package
        if package is None or package.title != doc.get_title():
            name = self._gen_new_name(doc.get_title())
            if not name:
                raise Exception('Could not generate a unique name from the title or the GUID. Please choose a more unique title.')
            package_dict['name'] = name
        else:
            package_dict['name'] = package.name

        extras = {
            'guid': harvest_object.guid,
            'author': doc.get_author(),
            'publisher': doc.get_publisher(),
            'publication_place': doc.get_publication_place(),
            'publication_date': doc.get_publication_date()
        }

        ## Just add some of the metadata as extras, not the whole lot
        #for name in [
            ## Essentials
            #'spatial-reference-system',
            #'guid',
            ## Usefuls
            #'dataset-reference-date',
            #'metadata-language',  # Language
            #'metadata-date',  # Released
            #'coupled-resource',
            #'contact-email',
            #'frequency-of-update',
            #'spatial-data-service-type',
        #]:
            #extras[name] = iso_values[name]

        #if len(iso_values.get('progress', [])):
            #extras['progress'] = iso_values['progress'][0]
        #else:
            #extras['progress'] = ''

        #if len(iso_values.get('resource-type', [])):
            #extras['resource-type'] = iso_values['resource-type'][0]
        #else:
            #extras['resource-type'] = ''

        #extras['licence'] = iso_values.get('use-constraints', '')

        #def _extract_first_license_url(licences):
            #for licence in licences:
                #o = urlparse(licence)
                #if o.scheme and o.netloc:
                    #return licence
            #return None

        #if len(extras['licence']):
            #license_url_extracted = _extract_first_license_url(extras['licence'])
            #if license_url_extracted:
                #extras['licence_url'] = license_url_extracted

        #extras['access_constraints'] = iso_values.get('limitations-on-public-access', '')

        # Grpahic preview
        #browse_graphic = iso_values.get('browse-graphic')
        #if browse_graphic:
            #browse_graphic = browse_graphic[0]
            #extras['graphic-preview-file'] = browse_graphic.get('file')
            #if browse_graphic.get('description'):
                #extras['graphic-preview-description'] = browse_graphic.get('description')
            #if browse_graphic.get('type'):
                #extras['graphic-preview-type'] = browse_graphic.get('type')

        # Save responsible organization roles
        #if iso_values['responsible-organisation']:
            #parties = {}
            #for party in iso_values['responsible-organisation']:
                #if party['organisation-name'] in parties:
                    #if not party['role'] in parties[party['organisation-name']]:
                        #parties[party['organisation-name']].append(party['role'])
                #else:
                    #parties[party['organisation-name']] = [party['role']]
            #extras['responsible-party'] = [{'name': k, 'roles': v} for k, v in parties.iteritems()]

        #resource_locators = iso_values.get('resource-locator', []) +\
            #iso_values.get('resource-locator-identification', [])

        url = doc.get_hyperlink().strip()
        if url:
            resource = {}
            resource['format'] = "PDF"  # probably we should have an heuristic here

            resource.update(
                {
                    'url': url,
                    'name': doc.get_text_type() or p.toolkit._('Unnamed resource'),
                    'description': doc.get_paper_type() or p.toolkit._('Unnamed resource')
                })
            package_dict['resources'].append(resource)

        extras_as_dict = []
        for key, value in extras.iteritems():
            if isinstance(value, (list, dict)):
                extras_as_dict.append({'key': key, 'value': json.dumps(value)})
            else:
                extras_as_dict.append({'key': key, 'value': value})

        package_dict['extras'] = extras_as_dict

        return package_dict

    def _set_source_config(self, config_str):
        '''
        Loads the source configuration JSON object into a dict for
        convenient access
        '''
        if config_str:
            self.source_config = json.loads(config_str)
            log.debug('Using config: %r', self.source_config)
        else:
            self.source_config = {}

    def _get_object_extra(self, harvest_object, key):
        '''
        Helper function for retrieving the value from a harvest object extra,
        given the key
        '''
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

    def _get_user_name(self):
        '''
        Returns the name of the user that will perform the harvesting actions
        (deleting, updating and creating datasets)

        By default this will be the internal site admin user. This is the
        recommended setting, but if necessary it can be overridden with the
        `ckanext.spatial.harvest.user_name` config option, eg to support the
        old hardcoded 'harvest' user:

           ckanext.spatial.harvest.user_name = harvest

        '''
        if self._user_name:
            return self._user_name

        self._site_user = p.toolkit.get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})

        config_user_name = config.get('ckanext.spatial.harvest.user_name')
        if config_user_name:
            self._user_name = config_user_name
        else:
            self._user_name = self._site_user['name']

        return self._user_name

    def handle_groups(self, harvest_object, doc):

        validated_groups = []

        if 'topic_group_mapping' in self.source_config:
            for groupname, codelist in self.source_config['topic_group_mapping'].iteritems():
                addgroup = False
                for code in codelist:
                    if addgroup:  # no need to search further matches for this group
                        break
                    for topic in doc.get_topics():
                        topic_code = topic[:4]
                        if topic_code == code:
                            log.debug('Found matching topic code %s' % code)
                            addgroup = True
                            break

                if addgroup:
                    log.info('Adding group %s ' % groupname)

                    try:
                        context = {'model': model, 'session': Session, 'user': 'harvest'}
                        data_dict = {'id': groupname}
                        get_action('group_show')(context, data_dict)
                        validated_groups.append({'name': groupname})
                    except NotFound:
                        log.warning('Group %s is not available' % (groupname))

        return validated_groups
