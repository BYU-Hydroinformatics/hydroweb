from tethys_sdk.base import TethysAppBase, url_map_maker

from tethys_sdk.app_settings import CustomSetting

class Hydroweb(TethysAppBase):
    """
    Tethys app class for Hydroweb.
    """

    name = 'Hydroweb'
    index = 'hydroweb:home'
    icon = 'hydroweb/images/icon.gif'
    package = 'hydroweb'
    root_url = 'hydroweb'
    color = '#2c3e50'
    description = ''
    tags = ''
    enable_feedback = False
    feedback_emails = []

    def url_maps(self):
        """
        Add controllers
        """
        UrlMap = url_map_maker(self.root_url)

        url_maps = (
            UrlMap(
                name='home',
                url='hydroweb',
                controller='hydroweb.controllers.home'
            ),
            UrlMap(
                name='getVirtualStationData',
                url='getVirtualStationData/',
                controller='hydroweb.controllers.getVirtualStationData'
            ),
        )

        return url_maps


    def custom_settings(self):
        custom_settings = (

            CustomSetting(
                name='Hydroweb Username',
                type = CustomSetting.TYPE_STRING,
                description='Hydroweb Username',
                required=False
            ),
            CustomSetting(
                name='Hydroweb Password',
                type = CustomSetting.TYPE_STRING,
                description='Hydroweb Password',
                required=False
            ),
        )
        return custom_settings