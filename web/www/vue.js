'use strict';

let router ;
let app ;
let wd = new WikiData() ;

$(document).ready ( function () {
    vue_components.toolname = 'cersei' ;
    Promise.all ( [
        vue_components.loadComponents ( ['wd-date','wd-link','tool-translate','tool-navbar','commons-thumbnail','widar','autodesc','typeahead-search','value-validator','batch-navigator',
            'vue_components/main-page.html',
            'vue_components/scraper-page.html',
            ] )
    ] )
    .then ( () => {
        wd_link_wd = wd ;
        const routes = [
            { path: '/', component: MainPage , props:true },
            { path: '/scraper/:scraper_id', component: ScraperPage , props:true },
            { path: '/scraper/:scraper_id/:start', component: ScraperPage , props:true },
        ] ;
        router = new VueRouter({routes}) ;
        app = new Vue ( { router } ) .$mount('#app') ;
        $('#help_page').attr('href','https://meta.wikimedia.org/wiki/Cersei');
    } ) ;

} ) ;
