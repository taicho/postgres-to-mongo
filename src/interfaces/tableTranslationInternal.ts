import { TableTranslation } from './tableTranslation';

export interface TableTranslationInternal extends TableTranslation {
    embedInParsed: string[];
    isSelfEmbed: boolean;
    hasEmbed: boolean;
}
