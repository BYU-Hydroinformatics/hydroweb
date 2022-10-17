from tethys_sdk.base import TethysAppBase, url_map_maker

from tethys_sdk.app_settings import CustomSetting, PersistentStoreDatabaseSetting

class Hydroweb(TethysAppBase):
    """
    Tethys app class for Hydroweb.
    """

    name = 'Hydroweb'
    index = 'home'
    icon = 'hydroweb/images/icon.gif'
    package = 'hydroweb'
    root_url = 'hydroweb'
    color = '#2c3e50'
    description = ''
    tags = ''
    enable_feedback = False
    feedback_emails = []

    controller_modules = ['controllers', 'consumers', ]

    # def url_maps(self):
    #     """
    #     Add controllers
    #     """
    #     UrlMap = url_map_maker(self.root_url)

    #     url_maps = (
    #         UrlMap(
    #             name='home',
    #             url='hydroweb',
    #             controller='hydroweb.controllers.home'
    #         ),
    #         UrlMap(
    #             name='getVirtualStationData',
    #             url='getVirtualStationData/',
    #             controller='hydroweb.controllers.getVirtualStationData'
    #         ),
    #         UrlMap(
    #             name='getVirtualStations',
    #             url='getVirtualStations/',
    #             controller='hydroweb.controllers.virtual_stations'
    #         ),
    #         UrlMap(
    #             name='saveHistoricalSimulationData',
    #             url='saveHistoricalSimulationData/',
    #             controller='hydroweb.controllers.saveHistoricalSimulationData'
    #         ),
    #         UrlMap(
    #             name='executeBiasCorrection',
    #             url='executeBiasCorrection/',
    #             controller='hydroweb.controllers.executeBiasCorrection'
    #         ),
    #         UrlMap(
    #             name='saveForecastData',
    #             url='saveForecastData/',
    #             controller='hydroweb.controllers.saveForecastData'
    #         ),
    #         UrlMap(
    #             name='data_notification',
    #             url='data-notification/notifications',
    #             controller='hydroweb.consumers.DataConsumer',
    #             protocol='websocket'
    #         ),
    #     )

    #     return url_maps


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

    def persistent_store_settings(self):
        """
        Add one or more persistent_stores.
        """
        # Create a new persistent store (database)
        stores = (
            PersistentStoreDatabaseSetting(
                name='virtual_stations',
                initializer='hydroweb.init_stores.init_flooded_addresses_db',
                spatial=True,
                required=True,
            ),
        )

        return stores