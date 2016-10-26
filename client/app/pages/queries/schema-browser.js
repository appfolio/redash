import template from './schema-browser.html';

function SchemaBrowserCtrl($scope) {
  'ngInject';

  this.showTable = (table) => {
    table.collapsed = !table.collapsed;
    $scope.$broadcast('vsRepeatTrigger');
  };

  this.getSize = (table) => {
    let size = 18;

    if (!table.collapsed) {
      size += 18 * table.columns.length;
    }

    return size;
  };

  this.filterPrivateTables = table => !table.name.split('.').slice(-1)[0].startsWith('_');
}

const SchemaBrowser = {
  bindings: {
    schema: '<',
    onRefresh: '&',
  },
  controller: SchemaBrowserCtrl,
  template,
};

export default function (ngModule) {
  ngModule.component('schemaBrowser', SchemaBrowser);
}
