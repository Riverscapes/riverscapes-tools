import os
import re
import json
from datetime import datetime
from dateutil.parser import parse as dateparse
import semver
from rsxml import Logger


def sanitize_version(version: str) -> str:
    """trailing zeros in versions are not allowed
    """
    return re.sub(r'\b0+([0-9])', r'\1', version.strip())


def format_date(date: datetime) -> str:
    """_summary_

    Args:
        date (datetime): _description_

    Returns:
        str: _description_
    """
    return date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3]


def verify_guid(guid: str):
    """ Really simple GUID validation. Just checks if it's a 24 character string of hex characters
    """
    if not re.match(r'^[a-f0-9-]{36}$', guid):
        return False
    return True


class RiverscapesProject:
    """This is just a helper class to make some of the RiverscapesAPI calls easier to use

    Returns:
        _type_: A RiverscapesProject object
    """

    def __init__(self, proj_obj):
        """ Note that we do not check for the existence of the keys in the proj_obj. This is because the API is expected to return a 
        consistent structure. If it doesn't then we want to know about it. 

        For example, you don not NEED to return "id" from your graphql query (even though you always should):

        THIS IS ONLY A CONVENIENCE CLASS. IT DOES NOT VALIDATE THE INPUTS. IT ASSUMES THE INPUTS ARE VALID.
        """
        log = Logger('RiverscapesProject')

        try:
            self.json = proj_obj
            self.id = proj_obj['id'] if 'id' in proj_obj else None
            self.name = proj_obj['name'] if 'name' in proj_obj else None
            self.created_date = dateparse(proj_obj['createdOn']) if 'createdOn' in proj_obj else None
            self.updated_date = dateparse(proj_obj['updatedOn']) if 'updatedOn' in proj_obj else None

            self.visibility = proj_obj['visibility'] if 'visibility' in proj_obj else None
            # Turn the meta into a dictionary
            self.project_meta = {x['key']: x['value'] for x in proj_obj['meta']}
            # make a lowercase version of the meta and strip away spaces, dashes, and underscores
            self.project_meta_lwr = {x['key'].lower().replace(' ', '').replace('-', '').replace('_', ''): x['value'] for x in proj_obj['meta']}
            self.huc = self.project_meta_lwr['huc'] if 'huc' in self.project_meta_lwr else None
            try:
                if 'modelversion' in self.project_meta_lwr:
                    cleaned_version = sanitize_version(self.project_meta_lwr['modelversion'])
                    self.model_version = semver.VersionInfo.parse(sanitize_version(cleaned_version))
                else:
                    self.model_version = None
            except Exception as error:
                log.error(f"Error parsing model version: {error}")
                log.error(f"   Model version found: {self.project_meta_lwr['modelversion']}")
                self.model_version = None

            self.tags = proj_obj['tags'] if 'tags' in proj_obj else []
            self.project_type = proj_obj['projectType']['id'] if 'projectType' in proj_obj and 'id' in proj_obj['projectType'] else None

        except Exception as error:
            raise Exception(f"Error parsing project RiverscapesProject object: {error}") from error


class RiverscapesProjectType:
    """_summary_
    """

    def __init__(self, proj_type_obj):
        """_summary_

        Args:
            proj_type_obj (_type_): _description_
        """
        self.json = proj_type_obj
        self.id = proj_type_obj['id'] if 'id' in proj_type_obj else None
        self.machine_name = proj_type_obj['machineName'] if 'machineName' in proj_type_obj else None
        self.name = proj_type_obj['name'] if 'name' in proj_type_obj else None
        self.tags = proj_type_obj['tags'] if 'tags' in proj_type_obj else None
        self.description = proj_type_obj['description'] if 'description' in proj_type_obj else None
        # Turn the meta into a dictionary
        self.project_meta = {x['key']: x['value'] for x in proj_type_obj['meta']}


class RiverscapesSearchParams:
    """ Search params are a bit of a pain to work with. This class will help us validate the input parameters.

            # search_params = {
        # keywords: "",          # [String]. Will search inside name, description, summary, meta data keys, metadata values, id and tags
        # name: "",              # [String]. Will search within the project name only
        # "editableOnly": True,  # [Boolean] Filter to Only items that I can edit as a user
        # "createdOn": {
        #     "from": "2024-01-01T00:00:00Z",
        # },                     # [SearchDate] Search by date {from, to}. If both from AND to are provided it will search inside a window.
        #                                       Otherwise it will search before, or after the date provided in the from or to field respecitvely
        # "updatedOn": {"from": "" },  [SearchDate] # search by date {from, to}
        # collection: "ID" # Filter to projects inside a collection
        # bbox: [minLng, minLat, maxLng, maxLat]
        # "projectTypeId": "vbet",
        # "meta": [
        #     {
        #         "key": "Runner",
        #         "value": "Cybercastor",
        #     },
        #     # {
        #     #     "key": "ModelVersion",
        #     #     "value": "3.2.0",
        #     # }
        # ],
        # "tags": ["tag1", "tag2"],  # AND query for tags
        # "ownedBy": {
        #     "id": "USER/ORGID",
        #     "type": "USER"  # or "ORGANIZATION"
        # }
        # }

    """

    def __init__(self, input_obj):
        self.log = Logger('SearchParams')
        if not isinstance(input_obj, dict):
            raise ValueError("Input must be a dictionary")

        self.original_json = input_obj
        # If input_obj is empty then we can just return an empty search params object
        if len(input_obj) == 0:
            self.log.warning("Empty search parameters object. This will return all projects. This will take a long time and may not be what you want. Use with caution.")

        self.keywords = input_obj.get('keywords', None)
        self.name = input_obj.get('name', None)
        self.editableOnly = input_obj.get('editableOnly', None)

        self.createdOnFrom = None
        self.createdOnTo = None
        self.updatedOnFrom = None
        self.updatedOnTo = None
        if 'createdOn' in input_obj:
            if 'from' in input_obj['createdOn']:
                self.createdOnFrom = dateparse(input_obj['createdOn'].get('from', None))
            if 'to' in input_obj['createdOn']:
                self.createdOnTo = dateparse(input_obj['createdOn'].get('to', None))
        if 'updatedOn' in input_obj:
            if 'from' in input_obj['updatedOn']:
                self.updatedOnFrom = dateparse(input_obj['updatedOn'].get('from', None))
            if 'to' in input_obj['updatedOn']:
                self.updatedOnTo = dateparse(input_obj['updatedOn'].get('to', None))

        self.collection = input_obj.get('collection', None)
        self.bbox = input_obj.get('bbox', None)
        self.projectTypeId = input_obj.get('projectTypeId', None)
        self.meta = input_obj.get('meta', None)
        self.tags = input_obj.get('tags', None)
        self.ownedBy = input_obj.get('ownedBy', None)

        # Throw an error of the dictionary contains any keys that are not in the list above
        for key in input_obj:
            if key not in ['keywords', 'name', 'editableOnly', 'createdOn', 'updatedOn', 'collection', 'bbox', 'projectTypeId', 'meta', 'tags', 'ownedBy']:
                raise ValueError(f"Invalid search parameter: {key}")

        # Make sure we have something to work with here
        self.validate()

    def to_gql(self):
        """This is where we output the search params the way the API expects them, converting them from pythonic to GQL

        Returns:
            _type_: _description_
        """
        initial = {
            "keywords": self.keywords,
            "name": self.name,
            "editableOnly": self.editableOnly,
            "createdOn": {
                "from": format_date(self.createdOnFrom) if self.createdOnFrom else None,
                "to": format_date(self.createdOnTo) if self.createdOnTo else None
            },
            "updatedOn": {
                "from": format_date(self.updatedOnFrom) if self.updatedOnFrom else None,
                "to": format_date(self.updatedOnTo) if self.updatedOnTo else None
            },
            "collection": self.collection,
            "bbox": self.bbox,
            "projectTypeId": self.projectTypeId,
            "meta": [{"key": k, "value": v} for k, v in self.meta.items()] if self.meta else None,
            "tags": self.tags,
            "ownedBy": self.ownedBy
        }
        # Now remove any None values
        sanitized = {k: v for k, v in initial.items() if v is not None}
        if "createdOn" in sanitized:
            sanitized["createdOn"] = {k: v for k, v in sanitized["createdOn"].items() if v is not None}
            if sanitized["createdOn"] == {}:
                del sanitized["createdOn"]
        if "updatedOn" in sanitized:
            sanitized["updatedOn"] = {k: v for k, v in sanitized["updatedOn"].items() if v is not None}
            if sanitized["updatedOn"] == {}:
                del sanitized["updatedOn"]
        return sanitized

    def validate(self):
        """ We can save a lot of grief here by validating the input parameters
        """
        if self.keywords is not None:
            if not isinstance(self.keywords, str):
                raise ValueError("keywords must be a string")
            # Using keywords programmatically should come with a warning about fuzzy search results for now
            self.log.warning("Using keywords in search is not recommended. It will return fuzzy search results and may not be accurate. Use with caution.")

        if self.name is not None:
            if not isinstance(self.name, str):
                raise ValueError("name must be a string")
            # Using name programmatically should come with a warning about fuzzy search results for now
            self.log.warning("Using name in search is not recommended. It will return fuzzy search results and may not be accurate. Use with caution.")

        if self.editableOnly is not None and not isinstance(self.editableOnly, bool):
            raise ValueError("editableOnly must be a boolean")
        # Make sure createdOn is a valid python date
        if self.createdOnFrom is not None and not isinstance(self.createdOnFrom, datetime):
            raise ValueError("createdOn.from must be a valid date")
        if self.createdOnTo is not None and not isinstance(self.createdOnTo, datetime):
            raise ValueError("createdOn.to must be a valid date")
        if self.updatedOnFrom is not None and not isinstance(self.updatedOnFrom, datetime):
            raise ValueError("updatedOn.from must be a valid date")
        if self.updatedOnTo is not None and not isinstance(self.updatedOnTo, datetime):
            raise ValueError("updatedOn.to must be a valid date")
        if self.createdOnFrom is not None and self.createdOnTo is not None and self.createdOnFrom > self.createdOnTo:
            raise ValueError("createdOn.from must be before createdOn.to")
        if self.updatedOnFrom is not None and self.updatedOnTo is not None and self.updatedOnFrom > self.updatedOnTo:
            raise ValueError("updatedOn.from must be before updatedOn.to")

        if self.collection is not None and not verify_guid(self.collection):
            raise ValueError("collection must be a valid GUID")
        if self.projectTypeId is not None and not isinstance(self.projectTypeId, str):
            raise ValueError("projectTypeId must be a string")
        if self.tags is not None and not isinstance(self.tags, list):
            raise ValueError("tags must be a list of strings")
        if self.tags is not None and not all(isinstance(x, str) for x in self.tags):
            raise ValueError("tags must be a list of strings")

        # meta must a dictionary of key/value pairs of strings
        if self.meta is not None:
            if not isinstance(self.meta, dict):
                raise ValueError("meta must be a dictionary of key/value string pairs")
            elif not all(isinstance(x, str) for x in self.meta.keys()):
                raise ValueError("meta must be a dictionary of key/value string pairs")
            elif not all(isinstance(x, str) for x in self.meta.values()):
                raise ValueError("meta must be a dictionary of key/value string pairs")

        # ownedBy must be a dictionary
        if self.ownedBy is not None:
            if not isinstance(self.ownedBy, dict):
                raise ValueError("ownedBy must be a dictionary")
            elif not verify_guid(self.ownedBy.get('id', '')):
                raise ValueError("ownedBy.id must be a valid GUID")
            elif self.ownedBy.get('type', '') not in ['USER', 'ORGANIZATION']:
                raise ValueError("ownedBy.type must be either 'USER' or 'ORGANIZATION'")

        # Must take the form: [minLng, minLat, maxLng, maxLat]
        if self.bbox is not None:
            if not isinstance(self.bbox, list) or len(self.bbox) != 4:
                raise ValueError("bbox must be a list of the form [minLng, minLat, maxLng, maxLat] (it must be a list with 4 values)")
            elif not all(isinstance(x, (int, float)) for x in self.bbox):
                raise ValueError("bbox must be a list of the form [minLng, minLat, maxLng, maxLat] (the values must be numbers)")
            elif self.bbox[0] >= self.bbox[2] or self.bbox[1] >= self.bbox[3]:
                raise ValueError("bbox must be a list of the form [minLng, minLat, maxLng, maxLat] (your minLng must be less than maxLng and minLat must be less than maxLat)")

        self.log.debug("Search parameters validated Successfully!")

    @staticmethod
    def load_from_json(json_path: str):
        """_summary_

        Args:
            json_obj (_type_): _description_

        Returns:
            _type_: _description_
        """
        log = Logger('SearchParams')
        search_params = None
        if not os.path.exists(json_path):
            raise Exception(f"Could not find the file: {json_path}. Create this file and put a search inside it")

        with open(json_path, 'r', encoding='utf8') as f:
            search_params = RiverscapesSearchParams(json.load(f))
        log.debug(f"Successfully loaded search parameters from: {json_path}")
        log.debug(f"Search parameters: {json.dumps(search_params.to_gql(), indent=2)}")
        return search_params
